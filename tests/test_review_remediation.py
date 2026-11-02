"""PR #3 independent review: resource boundaries and bounded process cleanup."""
import asyncio
import sys
import tracemalloc

import httpx
import pytest

from flink_mcp import client, kube, prometheus, server
from flink_mcp.client import FlinkClient, FlinkHTTPError
from flink_mcp.kube import KubeAuth, KubeClient, KubeConfig, KubeError
from flink_mcp.logs import LogTail
from flink_mcp.prometheus import PromClient, PromSettings, PromError
from test_hardening import Stream, log_client


def plugin(tmp_path, source):
    path = tmp_path / "plugin.py"
    path.write_text(source, encoding="utf-8")
    return KubeAuth(KubeConfig("http://k", exec_spec={"command": sys.executable, "args": [str(path)]}))


def record_processes(monkeypatch):
    processes = []
    create = asyncio.create_subprocess_exec
    async def record(*args, **kwargs):
        proc = await create(*args, **kwargs)
        processes.append(proc)
        return proc
    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    return processes


@pytest.mark.parametrize("source,expected", [
    ("import os,time; os.close(1); os.close(2); time.sleep(60)", "timed out"),
    ("import sys; sys.stdout.buffer.write(b'x'*2000000); sys.stdout.flush()", "exceeded"),
    ("import sys; sys.stderr.buffer.write(b'x'*2000000); sys.stderr.flush()", "exceeded"),
    ("import sys; sys.stderr.write('synthetic-secret'); sys.exit(7)", "stderr withheld"),
], ids=["closed-pipes-stalled-child", "stdout-overflow", "stderr-overflow", "failure"])
async def test_real_plugin_failure_paths_release_process_and_pipes(tmp_path, monkeypatch, source, expected):
    monkeypatch.setattr(kube, "EXEC_TIMEOUT_SECONDS", 0.3)
    processes = record_processes(monkeypatch)
    with pytest.raises(KubeError, match=expected) as error:
        await asyncio.wait_for(plugin(tmp_path, source).token(), 3)
    assert "synthetic-secret" not in str(error.value)
    assert processes[0].returncode is not None
    assert processes[0]._transport.is_closing()


async def test_descendant_keeps_inherited_pipes_open(tmp_path, monkeypatch):
    # The descendant exits by itself. No process tree/group assumptions or external programs.
    source = "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(1)'])"
    monkeypatch.setattr(kube, "EXEC_TIMEOUT_SECONDS", 0.1)
    processes = record_processes(monkeypatch)
    started = asyncio.get_running_loop().time()
    try:
        with pytest.raises(KubeError, match="timed out"):
            await asyncio.wait_for(plugin(tmp_path, source).token(), 2)
        assert processes[0].returncode is not None
        assert processes[0]._transport.is_closing()
        assert asyncio.get_running_loop().time() - started < 0.7
    finally:
        await asyncio.sleep(1.1)  # let the intentionally finite descendant exit


class FakeProcess:
    def __init__(self, *, failure=False, reap_delay=0, never_reap=False):
        self.stdout, self.stderr = asyncio.StreamReader(), asyncio.StreamReader()
        if failure:
            self.stdout.set_exception(RuntimeError("synthetic-secret"))
        self.returncode = None
        self.killed = asyncio.Event()
        self.closed = False
        self.reap_delay, self.never_reap = reap_delay, never_reap
        self._transport = self

    def kill(self):
        self.killed.set()

    def close(self):
        self.closed = True
        self.stdout.feed_eof()
        self.stderr.feed_eof()

    async def wait(self):
        await self.killed.wait()
        if self.never_reap:
            await asyncio.Event().wait()
        await asyncio.sleep(self.reap_delay)
        self.returncode = -9
        return self.returncode


async def fake_auth(monkeypatch, proc):
    async def create(*args, **kwargs):
        return proc
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    return KubeAuth(KubeConfig("http://k", exec_spec={"command": sys.executable}))


async def test_reader_task_failure_withholds_error_and_closes_other_reader(monkeypatch):
    proc = FakeProcess(failure=True)
    auth = await fake_auth(monkeypatch, proc)
    before = set(asyncio.all_tasks())
    with pytest.raises(KubeError, match="could not be read") as error:
        await asyncio.wait_for(auth.token(), 1)
    assert "synthetic-secret" not in str(error.value)
    assert proc.closed and proc.returncode is not None
    await asyncio.sleep(0)
    assert not (set(asyncio.all_tasks()) - before)


async def test_repeated_cancellation_cannot_interrupt_cleanup(monkeypatch):
    proc = FakeProcess(reap_delay=0.08)
    auth = await fake_auth(monkeypatch, proc)
    before = set(asyncio.all_tasks())
    task = asyncio.create_task(auth.token())
    await asyncio.sleep(0.01)
    task.cancel()
    await proc.killed.wait()
    for _ in range(4):
        task.cancel()
        await asyncio.sleep(0.01)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert proc.closed and proc.returncode is not None
    await asyncio.sleep(0)
    assert not (set(asyncio.all_tasks()) - before)


async def test_cleanup_reaping_has_its_own_deadline(monkeypatch):
    proc = FakeProcess(failure=True, never_reap=True)
    auth = await fake_auth(monkeypatch, proc)
    monkeypatch.setattr(kube, "EXEC_CLEANUP_SECONDS", 0.03)
    before = set(asyncio.all_tasks())
    with pytest.raises(KubeError, match="cleanup deadline"):
        await asyncio.wait_for(auth.token(), 1)
    assert proc.closed and proc.killed.is_set()
    await asyncio.sleep(0)
    assert not (set(asyncio.all_tasks()) - before)


async def test_cancelled_cleanup_task_does_not_spin_forever(monkeypatch):
    proc = FakeProcess(failure=True)
    auth = await fake_auth(monkeypatch, proc)
    close = kube._close_exec
    async def interrupted_cleanup(process, readers):
        await close(process, readers)
        raise asyncio.CancelledError
    monkeypatch.setattr(kube, "_close_exec", interrupted_cleanup)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(auth.token(), 1)
    assert proc.closed and proc.returncode is not None


@pytest.mark.parametrize("suffix", [b"", b"\n", b"\r", b"\r\n", b"x"])
async def test_exact_64k_logical_line_boundary(suffix):
    stream = Stream([b"x" * 65536, suffix])
    c, tail = log_client(stream), LogTail()
    try:
        assert await c.log_tail("/log", tail) is (suffix != b"x")
        assert tail.scanned == 1 and stream.closed
        assert len(tail.result()["lines"][0]) < 700
    finally:
        await c.aclose()


@pytest.mark.parametrize("ending", [b"\n", b"\r", b"\r\n", b""])
async def test_utf8_split_across_chunks_and_final_line(ending):
    data = "Hakkı Burak Alkış €".encode() + ending
    c, tail = log_client(Stream([bytes([b]) for b in data])), LogTail()
    try:
        assert await c.log_tail("/log", tail)
        assert tail.result()["lines"] == ["Hakkı Burak Alkış €"]
    finally:
        await c.aclose()


@pytest.mark.parametrize("data,limit,complete,lines", [
    (b"a\r\nb\r\n", 6, True, ["a", "b"]),
    (b"a\r\nb\r\n", 5, False, ["a", "b"]),
    (b"final", 5, True, ["final"]),
    (b"final!", 5, False, []),
])
async def test_total_boundary_counts_raw_bytes(data, limit, complete, lines):
    c, tail = log_client(Stream([data])), LogTail()
    try:
        assert await c.log_tail("/log", tail, max_bytes=limit) is complete
        assert tail.result()["lines"] == lines
    finally:
        await c.aclose()


async def test_timeout_after_partial_output_retains_complete_lines(monkeypatch):
    monkeypatch.setattr(client, "LOG_DEADLINE_SECONDS", 0.05)
    stream = Stream([b"first\nsecond\npartial"], delay=0.001)
    c, tail = log_client(stream), LogTail()
    try:
        assert not await asyncio.wait_for(c.log_tail("/log", tail), 1)
        assert tail.result()["lines"] == ["first", "second"]
        assert stream.closed
    finally:
        await c.aclose()


@pytest.mark.parametrize("cancel", [False, True])
async def test_oversized_single_chunk_remains_interruptible(monkeypatch, cancel):
    data = b"line\n" * 1000000
    monkeypatch.setattr(client, "LOG_DEADLINE_SECONDS", 30 if cancel else 0.1)
    stream = Stream([data])
    started = asyncio.Event()
    class ObservedTail(LogTail):
        def feed(self, line):
            super().feed(line)
            started.set()
    c, tail = log_client(stream), ObservedTail(lines=3)
    try:
        task = asyncio.create_task(c.log_tail("/log", tail))
        if cancel:
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        else:
            assert not await asyncio.wait_for(task, 1)
        assert 0 < tail.scanned < 1000000
        assert stream.closed and c._slots._value == 16
    finally:
        await c.aclose()


async def test_huge_chunk_does_not_get_copied_into_application_line_buffer():
    # Allocate the transport-owned chunk before tracing. This checks application
    # buffering overhead only, and deliberately makes no process-wide memory claim.
    data = b"x" * (8 * 1024 * 1024)
    c, tail = log_client(Stream([data])), LogTail()
    tracemalloc.start()
    try:
        assert not await c.log_tail("/log", tail)
        _, peak = tracemalloc.get_traced_memory()
        assert peak < 2 * 1024 * 1024
    finally:
        tracemalloc.stop()
        await c.aclose()


@pytest.mark.parametrize("kind", ["flink", "kube", "prometheus"])
async def test_json_limit_checked_before_copying_oversized_chunk(monkeypatch, kind):
    class CheckedBuffer(bytearray):
        def extend(self, data):
            assert len(self) + len(data) <= 128, "oversized chunk copied before size check"
            super().extend(data)
    module, cap = {
        "flink": (client, "MAX_JSON_BYTES"),
        "kube": (kube, "MAX_RESPONSE_BYTES"),
        "prometheus": (prometheus, "MAX_RESPONSE_BYTES"),
    }[kind]
    monkeypatch.setattr(module, cap, 128)
    monkeypatch.setattr(module, "bytearray", CheckedBuffer, raising=False)
    stream = Stream([b"x" * 100000])
    transport = httpx.MockTransport(lambda r: httpx.Response(200, stream=stream))
    if kind == "flink":
        c, error = FlinkClient("http://f", transport=transport), FlinkHTTPError
        call = c.overview()
    elif kind == "kube":
        c, error = KubeClient(KubeConfig("http://k"), transport=transport), KubeError
        call = c.get("/api/v1/pods")
    else:
        c, error = PromClient(PromSettings("http://p"), transport=transport), PromError
        call = c.query("up")
    try:
        with pytest.raises(error):
            await call
        assert stream.closed
    finally:
        await c.aclose()


async def test_noninteractive_plugin_cannot_read_mcp_stdin(tmp_path):
    auth = plugin(tmp_path, "import sys,json; assert sys.stdin.read() == ''; print(json.dumps({'status': {'token':'synthetic-token'}}))")
    assert await asyncio.wait_for(auth.token(), 3) == "synthetic-token"


async def test_mcp_log_result_explicitly_reports_incomplete(monkeypatch):
    monkeypatch.setattr(client, "LOG_DEADLINE_SECONDS", 0.05)
    stream = Stream([b"received\npartial"], delay=0.001)
    c = log_client(stream)
    async def get_client(cluster=None):
        return c
    monkeypatch.setattr(server, "get_client", get_client)
    try:
        result = await server.get_logs(file="flink.log")
        assert result["complete"] is False
        assert result["lines"] == ["received"]
        assert result["source"] == "jobmanager" and result["file"] == "flink.log"
    finally:
        await c.aclose()
