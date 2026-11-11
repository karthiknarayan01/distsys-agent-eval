# distsys-agent-eval

A benchmark of how well coding agents write fault-tolerant distributed systems code.

An agent gets a task, such as a broadcast node that must reach the whole cluster while the network
is partitioned, and writes a Go program in a fresh container. [Maelstrom](https://github.com/jepsen-io/maelstrom)
runs that program as a cluster several times under injected faults, and the attempt passes only if
every run is valid. No judge model is involved. Repeating attempts gives pass^k: how reliably an
agent solves a task, not just whether it can.

Status: under construction.

## Tasks

Each task is a directory in `tasks/`: a `task.toml` with the Maelstrom workload and flags, how many
runs in a row an attempt must pass, and the agent's time and step limits, and a `prompt.md` with
the problem. `tasks/workspace.md` describes the container and is the same for every task.

A task can also set targets for how many messages the nodes may send per operation and how long
a value may take to spread. And it can rename Maelstrom's message types and fields, so its
protocol isn't one an agent may have memorized: grading then puts `maelstrom-rename`
(`docker/rename/`) between Maelstrom and the program to translate.

The public challenges and their solutions are on GitHub, so an agent may pass a task from
memory. Some tasks therefore have a variant (`tasks/*-variant/`): the same problem under new
names and a different cluster size, say a box office issuing tickets instead of nodes generating
IDs. A variant uses its original's reference and mutants, with the names changed in the code to
match, so it is checked as thoroughly. `report` sets each original and its variant side by side;
an agent that does much worse on the variant was likely recalling rather than solving.

```sh
distsys-eval tasks          # list them
distsys-eval tasks echo     # everything the agent is told, including the exact grading command
```

## Agents

An agent gets the task's brief and the container's working directory, reachable only through
running a command, reading a file and writing a file. It works until it says it's done or runs out
of the task's time or steps, and writes a transcript of everything it did. It never sees the grade.
Any agent fits behind `Agent` in `distsys_eval/agent.py`.

The built-in one is [Shellmate](https://github.com/karthiknarayan01/Shellmate)'s agent loop,
pinned to a commit, with `shell`, `read_file` and `write_file` tools that work inside the container.
It runs a local Qwen model through Ollama, DeepSeek or Claude; the model is called from the host,
so the container itself never needs a network.

Any command-line coding agent can be run instead. Build an image from the sandbox image with the
agent installed, and give the command that starts it; the brief is in the file `$BRIEF`:

```sh
distsys-eval run --command 'claude -p "$(cat "$BRIEF")" --dangerously-skip-permissions' \
    --name claude-code --image my-sandbox-with-claude --network --env ANTHROPIC_API_KEY
```

The command runs in the working directory until it exits or the task's time runs out. Its output
is the transcript. Its steps and tokens aren't known to the harness, so they're recorded as 0 and
the step limit isn't enforced, and a command that exits with an error is recorded as an `error`,
since that is usually the agent's own setup failing. Such an agent calls its model from inside
the container, so `--network` gives the sandbox one; it could then fetch a solution as well, which
is worth keeping in mind when comparing its results with an agent's that had no network.

## Grading

An attempt is graded by building it with `go build` and running Maelstrom the task's m times,
stopping at the first run that isn't valid. It passes only if every run is valid. A failure gets
one category: `doesnt-build`, `crashed`, `fails-without-faults`, `fails-under-faults` (valid
again once the task's faults are switched off), or `too-slow` (valid, but over the task's
message or latency targets). A run Maelstrom couldn't decide is an `error` and isn't counted
against the agent.

```sh
distsys-eval grade echo path/to/solution    # grade a directory in a fresh sandbox
```

## Running an agent

`run` makes n attempts at each task, each in a fresh sandbox, and grades each one as it ends.

```sh
ollama pull qwen3.5:9b
distsys-eval run --provider ollama --attempts 5             # every task
distsys-eval run echo --provider deepseek --jobs 2          # one task, two attempts at a time
```

Everything goes into `results/<agent>/` (or `--out`):

```
results.jsonl          one line per attempt: outcome, failure category, steps, tokens, time
transcripts/echo-1.jsonl   everything the agent did in attempt 1 at echo
code/echo-1/           what it left in the working directory
```

An attempt the harness let down is recorded as an `error` and isn't scored: Docker failed, the
model couldn't be reached, or Maelstrom couldn't decide a run. Running the same command again
carries on: attempts already recorded are kept, and only missing ones and errors are run again.

## Reading the results

```sh
distsys-eval report results/shellmate-ollama-qwen3.5-9b     # a table per agent
distsys-eval report results/* --json                        # every number, for other tools
```

For each task the report gives pass^k: the chance that k of its attempts, drawn at random, all
pass. With c of n attempts passing it is C(c, k) / C(n, k), so pass^1 is the pass rate and pass^n
is 1 only if every attempt passed. An agent that solves a task now and then scores well at k = 1
and falls away as k grows; one that can be relied on stays up. The overall row is the mean over
tasks. Errors are left out of every number and counted on their own.

Each failed attempt is given one reason: the agent ran out of steps or time (or its reply hit the
model's output cap), or else the grader's category. The report also shows the mean steps and
minutes per attempt, and the tokens each task used.

## Trusting the grader

Every task carries code whose grade is known: `reference/`, a correct solution that must pass,
and `mutants/*.patch`, copies of it with one realistic bug each, which must fail (and not just
fail to build). `validate` grades them all and says which came out wrong. It takes a while, so
it isn't part of CI: run it after changing a task or the grader, locally or from the `validate`
workflow.

```sh
distsys-eval validate --jobs 3          # every task
distsys-eval validate unique-ids        # one task
```

A mutant caught once may still slip past a single run on another day, so each task's `runs` (m)
is measured rather than guessed. `calibrate` runs the reference and every mutant 20 times each,
takes the lower 95% bound on how often one run catches each mutant, and sets m to the fewest
runs that catch every mutant 99% of the time. The counts are kept in each task's
`calibration.toml`. It runs on demand from the `calibrate` workflow, since it takes about an hour.

```sh
distsys-eval calibrate --jobs 3 --write    # measure, then write calibration.toml and runs
```

## Development

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check . && python -m pytest -q
```

Every attempt runs in a container built from `docker/Dockerfile` (Go, a Java runtime and
Maelstrom 0.2.4). Build it once, then run the Docker tests against it:

```sh
distsys-eval build-image
SANDBOX_TEST_IMAGE=distsys-agent-eval/sandbox:0.1.0 python -m pytest -q -m docker
```
