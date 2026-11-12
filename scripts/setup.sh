# shellcheck shell=bash
# Setup steps for eval.sh. Source it after setting ROOT to the repository:
#
#   ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$ROOT/scripts/setup.sh"
#
#   install_package               .venv with the package installed (again when pyproject.toml changes)
#   ensure_docker_running         Docker's daemon answering, starting Docker Desktop on macOS if needed
#   ensure_sandbox_image          the sandbox image built (again when docker/ changes)
#   load_env_file                 export model API keys (ANTHROPIC_API_KEY, OPENAI_API_KEY, DEEPSEEK_API_KEY) from .env
#   qwen_for_this_machine         the Qwen model that fits this computer's memory
#   ensure_ollama_running         install Ollama (after asking) and start it if needed
#   ensure_model_downloaded M     pull model M unless Ollama already has it
#   say / fail                    a status line / an error line and exit 1
#
# Written for bash 3.2 (macOS's /bin/bash).

VENV="${DISTSYS_EVAL_VENV:-$ROOT/.venv}"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/distsys-agent-eval"
OLLAMA_URL="${OLLAMA_HOST:-127.0.0.1:11434}"
[[ "$OLLAMA_URL" == http* ]] || OLLAMA_URL="http://$OLLAMA_URL"

# --- the package ----------------------------------------------------------------------------

install_package() {
    # Reinstall only when pyproject.toml has changed since the last install.
    local stamp="$VENV/.distsys-eval-installed" want
    want="$(cksum < "$ROOT/pyproject.toml")"
    if [[ -x "$VENV/bin/distsys-eval" && -f "$stamp" && "$(cat "$stamp")" == "$want" ]]; then
        return
    fi
    if [[ ! -x "$VENV/bin/python" ]]; then
        local python
        python="$(find_python)" || fail "Python 3.11 or newer is needed. Install it (macOS: brew install python@3.13) and run this again."
        say "Creating a virtual environment with $("$python" --version)…"
        "$python" -m venv "$VENV"
    fi
    say "Installing the benchmark…"
    mkdir -p "$STATE_DIR"
    local log="$STATE_DIR/install.log"
    if ! { "$VENV/bin/python" -m pip install --upgrade pip &&
        "$VENV/bin/python" -m pip install -e "$ROOT"; } > "$log" 2>&1; then
        tail -n 20 "$log" >&2
        fail "Installing failed; the full output is in $log"
    fi
    printf '%s\n' "$want" > "$stamp"
}

find_python() {
    # The first python on PATH that is 3.11 or newer.
    local candidate
    for candidate in python3.13 python3.12 python3.11 python3; do
        if command -v "$candidate" >/dev/null 2>&1 &&
            "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
            command -v "$candidate"
            return 0
        fi
    done
    return 1
}

# --- Docker and the sandbox image -----------------------------------------------------------

ensure_docker_running() {
    command -v docker >/dev/null 2>&1 ||
        fail "Docker isn't installed. Get Docker Desktop from https://www.docker.com/products/docker-desktop and run this again."
    docker_up && return
    if [[ "$(uname)" == "Darwin" && -d /Applications/Docker.app ]]; then
        say "Starting Docker Desktop…"
        open -a Docker
    else
        fail "Docker is installed but its daemon isn't running. Start it and run this again."
    fi
    local waited=0
    until docker_up; do
        sleep 2
        waited=$((waited + 2))
        [[ $waited -lt 120 ]] || fail "Docker didn't start within 2 minutes. Open Docker Desktop, wait for it, and run this again."
    done
}

docker_up() {
    docker info >/dev/null 2>&1
}

ensure_sandbox_image() {
    # Build when the image is missing or docker/ has changed since it was built.
    local stamp="$STATE_DIR/image-built-from" want image
    want="$(cd "$ROOT/docker" && find . -type f | LC_ALL=C sort | xargs cat | cksum)"
    image="$("$VENV/bin/python" -c 'from distsys_eval.sandbox import IMAGE; print(IMAGE)')"
    if docker image inspect "$image" >/dev/null 2>&1 && [[ -f "$stamp" && "$(cat "$stamp")" == "$want" ]]; then
        return
    fi
    say "Building the sandbox image $image (once, and again when docker/ changes; a few minutes)…"
    mkdir -p "$STATE_DIR"
    local log="$STATE_DIR/image-build.log"
    if ! "$VENV/bin/distsys-eval" build-image > "$log" 2>&1; then
        tail -n 20 "$log" >&2
        fail "Building the sandbox image failed; the full output is in $log"
    fi
    printf '%s\n' "$want" > "$stamp"
}

# --- model API keys -------------------------------------------------------------------------

load_env_file() {
    # Export the model API keys from .env, parsed as KEY=value lines, never executed. A key
    # already set in the environment wins.
    local file="${DISTSYS_EVAL_ENV_FILE:-$ROOT/.env}" key value
    [[ -f "$file" ]] || return 0
    while IFS='=' read -r key value || [[ -n "$key" ]]; do
        key="${key#export }"
        value="${value%$'\r'}"
        value="${value#\"}"; value="${value%\"}"
        case "$key" in
            DEEPSEEK_API_KEY | ANTHROPIC_API_KEY | OPENAI_API_KEY) [[ -n "${!key:-}" || -z "$value" ]] || export "$key=$value" ;;
        esac
    done < "$file"
}

# --- Ollama ---------------------------------------------------------------------------------

qwen_for_this_machine() {
    # qwen3.5:9b needs about 8 GB with its context; below 12 GB of memory, the 4B model.
    local bytes=0
    if [[ "$(uname)" == "Darwin" ]]; then
        bytes="$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
    elif [[ -r /proc/meminfo ]]; then
        bytes="$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo)"
    fi
    if [[ "$bytes" -gt 0 && "$bytes" -lt $((12 * 1024 * 1024 * 1024)) ]]; then
        echo "qwen3.5:4b"
    else
        echo "qwen3.5:9b"
    fi
}

ensure_ollama_running() {
    command -v ollama >/dev/null 2>&1 || install_ollama
    ollama_up && return
    say "Starting Ollama…"
    if [[ "$(uname)" == "Darwin" && -d /Applications/Ollama.app ]]; then
        open -a Ollama
    else
        mkdir -p "$STATE_DIR"
        nohup ollama serve >> "$STATE_DIR/ollama.log" 2>&1 &
    fi
    local waited=0
    until ollama_up; do
        sleep 1
        waited=$((waited + 1))
        [[ $waited -lt 30 ]] || fail "Ollama didn't start within 30 seconds; see $STATE_DIR/ollama.log"
    done
}

ollama_up() {
    curl -fsS --max-time 2 "$OLLAMA_URL/api/version" >/dev/null 2>&1
}

install_ollama() {
    local answer
    read -r -p "Ollama runs open models on this computer and isn't installed. Install it now? [Y/n] " answer ||
        fail "Ollama isn't installed. Get it from https://ollama.com/download and run this again."
    [[ "${answer:-y}" =~ ^[Yy] ]] || fail "Install Ollama from https://ollama.com/download, or pick a hosted model instead."
    if [[ "$(uname)" == "Darwin" ]]; then
        command -v brew >/dev/null 2>&1 ||
            fail "Download the Ollama app from https://ollama.com/download, open it once, then run this again."
        brew install ollama
    else
        curl -fsSL https://ollama.com/install.sh | sh
    fi
}

ensure_model_downloaded() {
    # $1: the model, e.g. qwen3.5:9b
    ollama show "$1" >/dev/null 2>&1 && return
    say "Downloading $1 (once; several GB, so this can take a while)…"
    ollama pull "$1"
}

# --- output ---------------------------------------------------------------------------------

say() { printf '\033[36m›\033[0m %s\n' "$*"; }
fail() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
