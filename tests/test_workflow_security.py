"""Structural security invariants for CI and release workflow changes."""
from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    return yaml.load((ROOT / ".github/workflows" / name).read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def commands(job):
    return "\n".join(step.get("run", "") for step in job["steps"])


def test_all_action_references_are_immutable_and_checkout_credentials_not_persisted():
    for name in ("ci.yml", "publish.yml"):
        for job in load(name)["jobs"].values():
            for step in job["steps"]:
                if "uses" in step:
                    assert re.fullmatch(r"[\w.-]+/[\w./-]+@[a-f0-9]{40}", step["uses"])
                    if step["uses"].startswith("actions/checkout@"):
                        assert step["with"]["persist-credentials"] == "false"


def test_ci_matrix_and_security_gate():
    ci = load("ci.yml")
    matrix = ci["jobs"]["unit"]["strategy"]["matrix"]
    assert set(matrix["os"]) == {"ubuntu-latest", "windows-latest", "macos-latest"}
    assert set(matrix["python-version"]) == {"3.10", "3.11", "3.12", "3.13"}
    assert ci["permissions"] == {"contents": "read"}
    assert set(ci["jobs"]["security-package"]["needs"]) == {"unit", "oldest-sdk"}
    for name in ("integration", "kubernetes"):
        assert "security-package" in ci["jobs"][name]["needs"]
    scan = commands(ci["jobs"]["security-package"])
    for check in ("bandit==", "pip-audit==", "--require-hashes", "uv build", "twine=="):
        assert check in scan
    for job in ci["jobs"].values():
        assert "write" not in job.get("permissions", {}).values()
        assert "gh-action-pypi-publish" not in str(job)


def test_publishing_requires_release_and_successful_verification_build():
    workflow = load("publish.yml")
    assert workflow["on"] == {"release": {"types": ["published"]}}
    assert workflow["permissions"] == {"contents": "read"}
    jobs = workflow["jobs"]
    assert jobs["build"]["needs"] == "verify"
    assert jobs["publish"]["needs"] == "build"
    assert jobs["publish"]["environment"] == "pypi"
    assert jobs["publish"]["permissions"] == {"id-token": "write"}
    assert "permissions" not in jobs["verify"] and "permissions" not in jobs["build"]
    for check in ("pytest", "bandit==", "pip-audit==", "--require-hashes"):
        assert check in commands(jobs["verify"])
    upload = next(s for s in jobs["build"]["steps"] if s.get("uses", "").startswith("actions/upload-artifact@"))
    assert upload["with"]["if-no-files-found"] == "error"
    assert set(upload["with"]["path"].splitlines()) == {"dist/*.whl", "dist/*.tar.gz"}
    download = next(s for s in jobs["publish"]["steps"] if s.get("uses", "").startswith("actions/download-artifact@"))
    assert download["with"] == {"name": "dist", "path": "dist/"}
    assert upload["with"]["name"] == download["with"]["name"]
