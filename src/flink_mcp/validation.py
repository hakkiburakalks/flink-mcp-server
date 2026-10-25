"""Checks for values that end up inside a request path or a selector.

Tool arguments come from a language model, which may be following instructions hidden in a log
line or a job name. Whatever it passes must not be able to leave the endpoint it is meant for:
`job_id="../../x"` against a Kubernetes service proxy would otherwise reach other API paths with
the kubeconfig's credentials. Each helper accepts the characters real identifiers use and
rejects everything else instead of trying to escape it.
"""

from __future__ import annotations

import re

_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")  # Flink job and vertex ids are 32 hex digits
_TASKMANAGER = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.:\-]{0,255}$")  # e.g. 172.21.0.3:40653-43f9b4
_LOG_FILE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,255}$")
_NAMESPACE = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")  # DNS-1123 label
_NAME = re.compile(r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$")  # DNS-1123 subdomain
_UNSAFE_PATH = re.compile(r"[?#%\\\s]")


def _check(pattern: re.Pattern[str], value: object, what: str) -> str:
    if not isinstance(value, str) or not pattern.match(value) or ".." in value:
        shown = value if isinstance(value, str) and len(value) <= 40 else "that value"
        raise ValueError(f"'{shown}' is not a valid {what}")
    return value


def job_id(value: object) -> str:
    return _check(_ID, value, "Flink job id")


def vertex_id(value: object) -> str:
    return _check(_ID, value, "Flink vertex id")


def taskmanager_id(value: object) -> str:
    return _check(_TASKMANAGER, value, "TaskManager id")


def log_file(value: object) -> str:
    return _check(_LOG_FILE, value, "log file name")


def namespace(value: object) -> str:
    return _check(_NAMESPACE, value, "Kubernetes namespace")


def resource_name(value: object) -> str:
    return _check(_NAME, value, "Kubernetes resource name")


def safe_path(path: str) -> str:
    """Last line of defence for a request path built by this package: no query, fragment,
    escapes, backslashes or dot segments, so it cannot leave the base URL it is joined to."""
    if not path.startswith("/") or _UNSAFE_PATH.search(path) or any(
        segment in (".", "..") for segment in path.split("/")
    ):
        raise ValueError("refusing to request a path with unsafe characters")
    return path
