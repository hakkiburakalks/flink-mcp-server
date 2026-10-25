"""Reduce a Flink log file to the part worth reading, without holding the file in memory."""

from __future__ import annotations

import re
from collections import deque
from typing import Any

LEVELS = ("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
_ENTRY = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\S*\s+(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\b")
MAX_ENTRY_LINES = 25  # an entry is a log line plus its stack trace; keep the head of long traces
MAX_LINE_CHARS = 600


def normalize_level(level: str | None) -> str | None:
    if level is None:
        return None
    wanted = level.strip().upper()
    if wanted == "WARNING":
        wanted = "WARN"
    if wanted not in LEVELS:
        raise ValueError(f"level must be one of {', '.join(LEVELS)}")
    return wanted


class LogTail:
    """Feed it lines; it keeps the last `lines` lines of the entries that match.

    An entry is a timestamped log line together with the continuation lines after it
    (stack traces), so filtering by level never separates an error from its trace.
    """

    def __init__(self, lines: int = 200, level: str | None = None, search: str | None = None) -> None:
        self.lines = lines
        self.min_level = LEVELS.index(normalize_level(level)) if level else 0
        self.search = search.lower() if search else None
        self.scanned = 0
        self.matched = 0
        self.counts = {"WARN": 0, "ERROR": 0}
        self._kept: deque[list[str]] = deque()
        self._kept_lines = 0
        self._entry: list[str] = []
        self._entry_level = -1
        self._entry_skipped = 0

    def feed(self, line: str) -> None:
        self.scanned += 1
        if len(line) > MAX_LINE_CHARS:
            line = line[:MAX_LINE_CHARS] + " …"
        found = _ENTRY.match(line)
        if found:
            self._close()
            self._entry_level = LEVELS.index(found.group(1))
            self._entry = [line]
            self._entry_skipped = 0
            if found.group(1) in self.counts:
                self.counts[found.group(1)] += 1
        elif self._entry and self._entry_level < len(LEVELS):  # continuation of a timestamped entry
            if len(self._entry) < MAX_ENTRY_LINES:
                self._entry.append(line)
            else:
                self._entry_skipped += 1
        else:  # text before the first timestamped line, or logs in another format: one line each
            self._close()
            self._entry = [line]
            self._entry_level = len(LEVELS)  # unknown level: only the search decides
            self._entry_skipped = 0

    def _close(self) -> None:
        entry = self._entry
        if not entry:
            return
        text = "\n".join(entry).lower()
        level_ok = self._entry_level >= self.min_level
        if level_ok and (self.search is None or self.search in text):
            if self._entry_skipped:
                entry = entry + [f"    … {self._entry_skipped} more lines"]
            self.matched += 1
            self._kept.append(entry)
            self._kept_lines += len(entry)
            while self._kept_lines > self.lines and len(self._kept) > 1:
                self._kept_lines -= len(self._kept.popleft())
        self._entry = []

    def result(self) -> dict[str, Any]:
        self._close()
        out = [line for entry in self._kept for line in entry]
        if len(out) > self.lines:  # a single entry longer than the budget
            out = out[-self.lines:]
        return {
            "lines": out,
            "matched_entries": self.matched,
            "scanned_lines": self.scanned,
            "warnings_in_file": self.counts["WARN"],
            "errors_in_file": self.counts["ERROR"],
        }
