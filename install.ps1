<#
.SYNOPSIS
Installs flink-mcp-server (Windows) with uv.

.EXAMPLE
irm https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.ps1 | iex

.EXAMPLE
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.ps1))) -ClaudeCode -Url http://localhost:8081

.PARAMETER Url
Flink REST endpoint to register (default http://localhost:8081).

.PARAMETER ClaudeCode
Also register the server in Claude Code (needs the `claude` CLI).

.PARAMETER Uninstall
Remove flink-mcp-server.

Environment: FLINK_MCP_SOURCE overrides what is installed (a path or a git+ URL,
e.g. git+https://github.com/hakkiburakalks/flink-mcp-server@v0.1.0).
#>
param(
    [string]$Url = 'http://localhost:8081',
    [switch]$ClaudeCode,
    [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$Source = if ($env:FLINK_MCP_SOURCE) { $env:FLINK_MCP_SOURCE } else { 'git+https://github.com/hakkiburakalks/flink-mcp-server' }

# Windows PowerShell 5.1 turns any native stderr output into a terminating error under
# $ErrorActionPreference = 'Stop'; `claude mcp remove` writes to stderr when nothing is registered.
function Invoke-Optional([scriptblock]$Command) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command 2>$null | Out-Null } catch { } finally { $ErrorActionPreference = $previous }
}

function Add-ToPath([string]$dir) {
    if (($env:PATH -split ';') -notcontains $dir) { $env:PATH = "$dir;$env:PATH" }
}

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    if ($Uninstall) { Write-Host 'uv is not installed; nothing to uninstall.'; return }
    Write-Host 'uv not found, installing it from https://astral.sh/uv ...'
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
    # The uv installer puts binaries here; make them visible to this session.
    Add-ToPath (Join-Path $env:USERPROFILE '.local\bin')
    Add-ToPath (Join-Path $env:USERPROFILE '.cargo\bin')
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        throw 'uv was installed but is not on PATH; open a new terminal and rerun.'
    }
}

if ($Uninstall) {
    uv tool uninstall flink-mcp-server
    if (Get-Command claude -ErrorAction SilentlyContinue) { Invoke-Optional { claude mcp remove flink -s user } }
    Write-Host 'Removed flink-mcp-server.'
    return
}

Write-Host "Installing flink-mcp-server from $Source ..."
uv tool install --force $Source
if ($LASTEXITCODE -ne 0) { throw "uv tool install failed (exit $LASTEXITCODE)" }

$BinDir = (uv tool dir --bin).Trim()
$Bin = Join-Path $BinDir 'flink-mcp-server.exe'
if (-not (Test-Path $Bin)) { throw "Install finished but $Bin was not found." }
Write-Host ("Installed: " + (& $Bin --version))

if (($env:PATH -split ';') -notcontains $BinDir) {
    Write-Host "Note: $BinDir is not on your PATH. Run 'uv tool update-shell' and restart your terminal."
}

if ($ClaudeCode) {
    if (Get-Command claude -ErrorAction SilentlyContinue) {
        Invoke-Optional { claude mcp remove flink -s user }
        # claude is an npm .ps1 shim here and PowerShell swallows the `--` separator, so go through cmd.
        cmd.exe /d /s /c "claude mcp add -s user flink -e FLINK_REST_URL=$Url -- `"$Bin`""
        if ($LASTEXITCODE -ne 0) { throw "claude mcp add failed (exit $LASTEXITCODE)" }
        Write-Host "Registered 'flink' in Claude Code (user scope) -> $Url"
    } else {
        Write-Warning "The 'claude' CLI was not found; skipping Claude Code registration."
    }
}

$snippet = [ordered]@{
    mcpServers = [ordered]@{
        flink = [ordered]@{ command = $Bin; env = [ordered]@{ FLINK_REST_URL = $Url } }
    }
} | ConvertTo-Json -Depth 5
Write-Host "`nAdd it to other MCP clients (Claude Desktop, Cursor, ...) with:`n"
Write-Host $snippet
