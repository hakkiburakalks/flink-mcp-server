import httpx
import pytest

from flink_mcp.client import FlinkClient, FlinkHTTPError
from flink_mcp.logs import LogTail, normalize_level

HEAD = "2026-10-25 08:00:00,001 ERROR org.example.Job - failed"


def feed(tail: LogTail, *lines: str) -> dict:
    for line in lines:
        tail.feed(line)
    return tail.result()


def test_long_stack_traces_keep_their_head_and_say_how_much_was_dropped():
    out = feed(LogTail(lines=100), HEAD, *[f"\tat frame{i}" for i in range(60)])
    assert out["lines"][0] == HEAD
    assert out["lines"][-1].strip().endswith("more lines")
    assert len(out["lines"]) < 40


def test_a_single_entry_larger_than_the_budget_is_cut_from_the_end():
    out = feed(LogTail(lines=3), HEAD, *[f"\tat frame{i}" for i in range(10)])
    assert len(out["lines"]) == 3


def test_unstructured_lines_are_kept_and_only_search_filters_them():
    out = feed(LogTail(lines=10, search="needle"), "plain hay", "a needle here", "more hay")
    assert out["lines"] == ["a needle here"]


def test_very_long_lines_are_clipped():
    out = feed(LogTail(), "2026-10-25 08:00:00,001 INFO  x - " + "a" * 5000)
    assert len(out["lines"][0]) < 700


def test_level_names():
    assert normalize_level("warning") == "WARN"
    assert normalize_level(None) is None
    with pytest.raises(ValueError):
        normalize_level("loud")


async def test_log_tail_stops_at_the_byte_limit_and_reports_http_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/missing":
            return httpx.Response(404, json={"errors": ["File not found."]})
        return httpx.Response(200, text="\n".join(f"line {i}" for i in range(1000)))

    client = FlinkClient("http://flink", transport=httpx.MockTransport(handler))
    tail = LogTail(lines=5)
    assert await client.log_tail("/big", tail, max_bytes=100) is False
    assert tail.scanned < 1000
    with pytest.raises(FlinkHTTPError, match="File not found"):
        await client.log_tail("/missing", LogTail())
