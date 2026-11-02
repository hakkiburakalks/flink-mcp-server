"""Installer invocation tests with fake tools; never install or register anything."""
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SH = shutil.which("sh") or ("C:/Program Files/Git/usr/bin/sh.exe" if Path("C:/Program Files/Git/usr/bin/sh.exe").exists() else None)
PS = shutil.which("pwsh") or shutil.which("powershell")


@pytest.mark.skipif(not SH, reason="POSIX shell unavailable")
def test_shell_installer_syntax():
    subprocess.run([SH, "-n", str(ROOT / "install.sh")], check=True, capture_output=True)


@pytest.mark.skipif(not PS, reason="PowerShell unavailable")
def test_powershell_installer_passes_url_as_one_argument(tmp_path):
    fake = tmp_path / "flink.ps1"
    fake.write_text("Write-Output '0.4.4'", encoding="utf-8")
    harness = tmp_path / "check.ps1"
    # Functions shadow the external tools. No package installation or MCP registration occurs.
    harness.write_text(r'''
param($Installer, $Fake)
$ErrorActionPreference = 'Stop'
$env:FLINK_MCP_SOURCE = 'local-fixture'
$env:FLINK_MCP_VERSION = ''
function uv { if ($args[0] -eq 'tool' -and $args[1] -eq 'dir') { 'fixture' }; $global:LASTEXITCODE = 0 }
function Join-Path { param($Path, $ChildPath); $Fake }
$global:seen = @()
function claude { if ($args[1] -eq 'add') { $global:seen = @($args) }; $global:LASTEXITCODE = 0 }
$url = 'http://localhost:8081/?a=1&echo=INJECTION'
& $Installer -ClaudeCode -Url $url | Out-Null
if ($global:seen.Count -ne 9 -or $global:seen[6] -ne "FLINK_REST_URL=$url" -or $global:seen[7] -ne '--' -or $global:seen[8] -ne $Fake) { throw ($global:seen | ConvertTo-Json -Compress) }
''', encoding="utf-8")
    result = subprocess.run([PS, "-NoProfile", "-File", str(harness), str(ROOT / "install.ps1"), str(fake)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
