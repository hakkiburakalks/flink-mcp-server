#!/usr/bin/env sh
# Installs flink-mcp-server (macOS / Linux) with uv.
#
#   curl -LsSf https://raw.githubusercontent.com/hakkiburakalks/flink-mcp-server/main/install.sh | sh
#   curl -LsSf .../install.sh | sh -s -- --claude-code --url http://localhost:8081
#
# Options:
#   --url URL       Flink REST endpoint to register (default http://localhost:8081)
#   --claude-code   Also register the server in Claude Code (needs the `claude` CLI)
#   --uninstall     Remove flink-mcp-server
# Environment:
#   FLINK_MCP_SOURCE  What to install (default: the release on PyPI). Accepts a path or a git+ URL,
#                     e.g. git+https://github.com/hakkiburakalks/flink-mcp-server
set -eu

SOURCE="${FLINK_MCP_SOURCE:-flink-mcp-server}"
URL="http://localhost:8081"
CLAUDE=0
UNINSTALL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --url) [ $# -ge 2 ] || { echo "--url needs a value" >&2; exit 1; }; URL="$2"; shift 2 ;;
    --claude-code) CLAUDE=1; shift ;;
    --uninstall) UNINSTALL=1; shift ;;
    -h|--help) sed -n '2,13p' "$0" 2>/dev/null | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

say() { printf '%s\n' "$*"; }

if ! command -v uv >/dev/null 2>&1; then
  if [ "$UNINSTALL" = 1 ]; then say "uv is not installed; nothing to uninstall."; exit 0; fi
  say "uv not found, installing it from https://astral.sh/uv ..."
  if command -v curl >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- https://astral.sh/uv/install.sh | sh
  else
    say "Need curl or wget to install uv." >&2; exit 1
  fi
  # The uv installer puts binaries here; make them visible to this shell.
  PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
  export PATH
  command -v uv >/dev/null 2>&1 || { say "uv was installed but is not on PATH; open a new shell and rerun." >&2; exit 1; }
fi

if [ "$UNINSTALL" = 1 ]; then
  uv tool uninstall flink-mcp-server || true
  command -v claude >/dev/null 2>&1 && claude mcp remove flink -s user >/dev/null 2>&1 || true
  say "Removed flink-mcp-server."
  exit 0
fi

say "Installing flink-mcp-server from $SOURCE ..."
uv tool install --force "$SOURCE"

BIN_DIR="$(uv tool dir --bin)"
BIN="$BIN_DIR/flink-mcp-server"
[ -x "$BIN" ] || { say "Install finished but $BIN was not found." >&2; exit 1; }
say "Installed: $("$BIN" --version)"

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) say "Note: $BIN_DIR is not on your PATH. Run 'uv tool update-shell' and restart your shell." ;;
esac

if [ "$CLAUDE" = 1 ]; then
  if command -v claude >/dev/null 2>&1; then
    claude mcp remove flink -s user >/dev/null 2>&1 || true
    claude mcp add -s user flink -e "FLINK_REST_URL=$URL" -- "$BIN"
    say "Registered 'flink' in Claude Code (user scope) -> $URL"
  else
    say "The 'claude' CLI was not found; skipping Claude Code registration." >&2
  fi
fi

BIN_JSON="$(printf '%s' "$BIN" | sed 's/\\/\\\\/g')"  # escape backslashes (Git Bash on Windows)
cat <<EOF

Add it to other MCP clients (Claude Desktop, Cursor, ...) with:

  {
    "mcpServers": {
      "flink": {
        "command": "$BIN_JSON",
        "env": { "FLINK_REST_URL": "$URL" }
      }
    }
  }
EOF
