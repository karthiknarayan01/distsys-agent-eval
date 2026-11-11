#!/usr/bin/env bash
# Set everything up, run agents at the benchmark's tasks, and report the results.
#
#   ./eval.sh                                  asks which models, how many attempts, which tasks
#   ./eval.sh --models qwen3.5:9b --yes        no questions: these models, the defaults for the rest
#   ./eval.sh --models qwen3.5:4b,qwen3.5:9b --attempts 3 --tasks "echo unique-ids"
#   ./eval.sh --models deepseek                a hosted model (needs DEEPSEEK_API_KEY; paid)
#   ./eval.sh --dry-run ...                    print what it would run, and set nothing up
#
# A model is an Ollama model name (run on this computer), or deepseek or anthropic (hosted, with
# their API keys from the environment or .env). Defaults: the Qwen that fits this computer's
# memory, 5 attempts per task, 1 attempt at a time, every task.
#
# Before running, it:
#   1. creates .venv next to this script and installs the package (again whenever
#      pyproject.toml changes)
#   2. checks Docker is running (starting Docker Desktop on macOS) and builds the sandbox image
#      (again whenever docker/ changes)
#   3. for each local model: installs Ollama if it's missing (after asking), starts it, and
#      downloads the model if Ollama doesn't have it
#
# Each model's attempts go to results/<agent>/. An attempt the harness let down (Docker, the
# model server) is an error and isn't scored; those are run again, up to twice more. Stopped
# and started again, it carries on where it left off. At the end it writes results/report.txt,
# with every agent's table, and results/pass-k.svg.
#
# Written for bash 3.2 (macOS's /bin/bash): no mapfile, and no expanding an array that may be empty.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/setup.sh
source "$ROOT/scripts/setup.sh"

RESULTS="$ROOT/results"
EXTRA_PASSES=2  # runs again for attempts that errored

main() {
    local models="" attempts="" jobs="" tasks="" ask=1 dry_run=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --models) models="${2:-}"; shift ;;
            --attempts) attempts="${2:-}"; shift ;;
            --jobs) jobs="${2:-}"; shift ;;
            --tasks) tasks="${2:-}"; shift ;;
            --yes) ask=0 ;;
            --dry-run) dry_run=1 ;;
            -h | --help) sed -n '2,/^$/s/^# \{0,1\}//p' "$0"; return 0 ;;
            *) fail "Unknown option $1 (see ./eval.sh --help)" ;;
        esac
        shift
    done
    [[ -t 0 ]] || ask=0
    if [[ $ask -eq 1 ]]; then
        [[ -n "$models" ]] || models="$(choose_models)"
        [[ -n "$attempts" ]] || attempts="$(ask_value "Attempts per task" 5)"
        [[ -n "$jobs" ]] || jobs="$(ask_value "Attempts at a time (more needs more memory and CPU)" 1)"
        [[ -n "$tasks" ]] || tasks="$(ask_value "Task ids, space-separated (empty for every task)" "")"
    fi
    models="${models:-$(qwen_for_this_machine)}"
    attempts="${attempts:-5}"
    jobs="${jobs:-1}"
    [[ "$attempts" =~ ^[1-9][0-9]*$ && "$jobs" =~ ^[1-9][0-9]*$ ]] || fail "--attempts and --jobs must be positive numbers"

    local model_list=() model
    for model in $(echo "$models" | tr ',' ' '); do model_list+=("$model"); done
    [[ ${#model_list[@]} -gt 0 ]] || fail "No models to run"
    say "Running ${model_list[*]}: $attempts attempts at ${tasks:-every task}, $jobs at a time."
    if [[ $ask -eq 1 && $dry_run -eq 0 ]]; then
        local answer
        read -r -p "This can take hours. Start? [Y/n] " answer || answer=n
        [[ "${answer:-y}" =~ ^[Yy] ]] || return 0
    fi

    if [[ $dry_run -eq 0 ]]; then
        prepare "${model_list[@]}"
    fi
    for model in "${model_list[@]}"; do
        run_model "$model" "$attempts" "$jobs" "$tasks" "$dry_run"
    done
    report "$dry_run"
}

choose_models() {
    # A numbered menu on stderr; prints the chosen models, comma-separated, on stdout.
    local recommended options answer choice chosen=""
    recommended="$(qwen_for_this_machine)"
    options="qwen3.5:4b qwen3.5:9b qwen3.5:27b deepseek"
    {
        echo "Which models should the agent run on? (several are fine, e.g. 1,2)"
        echo "  1) qwen3.5:4b    open, on this computer"
        echo "  2) qwen3.5:9b    open, on this computer (needs about 12 GB of memory)"
        echo "  3) qwen3.5:27b   open, on this computer (needs about 32 GB of memory)"
        echo "  4) deepseek      hosted, needs DEEPSEEK_API_KEY, and is paid for per token"
        echo "  5) another Ollama model, by name"
    } >&2
    read -r -p "Models [$recommended, which fits this computer]: " answer >&2 || answer=""
    [[ -n "$answer" ]] || { echo "$recommended"; return; }
    for choice in $(echo "$answer" | tr ',' ' '); do
        case "$choice" in
            [1-4]) chosen="$chosen,$(echo "$options" | cut -d' ' -f"$choice")" ;;
            5) read -r -p "Ollama model name: " answer >&2; chosen="$chosen,$answer" ;;
            *) chosen="$chosen,$choice" ;;  # a model name typed in directly
        esac
    done
    echo "${chosen#,}"
}

ask_value() {
    # $1 the question, $2 the default. Prints the answer, or the default for an empty one.
    local answer
    read -r -p "$1 [${2:-none}]: " answer >&2 || answer=""
    echo "${answer:-$2}"
}

prepare() {
    # Everything the runs need, for these models.
    install_package
    load_env_file
    ensure_docker_running
    ensure_sandbox_image
    local model local_models=()
    for model in "$@"; do
        case "$model" in
            deepseek) ensure_key DEEPSEEK_API_KEY ;;
            anthropic) ensure_key ANTHROPIC_API_KEY ;;
            *) local_models+=("$model") ;;
        esac
    done
    [[ ${#local_models[@]} -gt 0 ]] || return 0
    ensure_ollama_running
    for model in "${local_models[@]}"; do ensure_model_downloaded "$model"; done
}

ensure_key() {
    # $1: the variable. If it isn't set, ask for it, for this run only; it isn't saved anywhere.
    [[ -z "${!1:-}" ]] || return 0
    [[ -t 0 ]] || fail "$1 isn't set. Put it in .env or the environment."
    local key
    read -r -s -p "$1 isn't set. Paste it (used for this run only): " key; echo
    [[ -n "$key" ]] || fail "$1 is needed for that model."
    export "$1=$key"
}

run_model() {
    # $1 model, $2 attempts, $3 jobs, $4 tasks (space-separated, may be empty), $5 dry run.
    local command=(run --attempts "$2" --jobs "$3")
    case "$1" in
        deepseek | anthropic) command+=(--provider "$1") ;;
        *) command+=(--provider ollama --model "$1") ;;
    esac
    local task
    for task in $4; do command+=("$task"); done
    if [[ $5 -eq 1 ]]; then
        echo "distsys-eval ${command[*]}"
        return
    fi
    local pass=0 log
    mkdir -p "$STATE_DIR"
    log="$STATE_DIR/run.log"
    while :; do
        if [[ $pass -eq 0 ]]; then say "$1: running its attempts…"; else say "$1: running the attempts that errored again…"; fi
        (cd "$ROOT" && "$VENV/bin/distsys-eval" "${command[@]}") | tee "$log" ||
            fail "$1: distsys-eval stopped; its output is above"
        tail -n 1 "$log" | grep -q " 0 errors" && return
        pass=$((pass + 1))
        [[ $pass -le $EXTRA_PASSES ]] || { say "$1: some attempts still errored; run this again later to retry them."; return; }
    done
}

report() {
    # $1 dry run. Every agent's table into results/report.txt, and the chart of them all.
    local dirs=() dir
    if [[ $1 -eq 1 ]]; then
        echo "distsys-eval report results/<each agent> --chart results/pass-k.svg > results/report.txt"
        return
    fi
    for dir in "$RESULTS"/*/; do [[ -f "$dir/results.jsonl" ]] && dirs+=("${dir%/}"); done
    [[ ${#dirs[@]} -gt 0 ]] || fail "No results to report"
    "$VENV/bin/distsys-eval" report "${dirs[@]}" --chart "$RESULTS/pass-k.svg" | tee "$RESULTS/report.txt"
    say "The report is in results/report.txt, the chart in results/pass-k.svg."
}

main "$@"
