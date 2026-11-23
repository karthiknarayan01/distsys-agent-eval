# Can open models write distributed systems code that survives network faults?

*Results from November 2026. The numbers are in the [README](README.md#-results-november-2026); this page explains them.*

Five large open-weights models each got 15 distributed systems tasks, from an echo server to read-committed transactions across a network split. Each wrote a Go program alone in a container. Jepsen's Maelstrom then ran the program as a cluster, injected faults, and checked every answer. Each model had 2 attempts per task, and every model was called on Together AI.

| model | 16k reply limit: pass^1 / pass^2 | cut-off attempts rerun at 64k: pass^1 / pass^2 |
|---|---|---|
| MiniMax M3.1 Flash | 1.00 / 1.00 (30 of 30) | — |
| DeepSeek V4.1 Flash 1017 | 1.00 / 1.00 (30 of 30) | — |
| DeepSeek V4 Pro 0813 | 0.93 / 0.86 (27 of 29) | 1.00 / 1.00 (29 of 29) |
| GLM-5.3 | 0.77 / 0.67 (23 of 30) | 1.00 / 1.00 (30 of 30) |
| Inkling | 0.80 / 0.71 (24 of 29) | — |

The short answer: yes, the best open models now write this code reliably. Four of the five pass every attempt once their replies have room to finish. Of the 14 attempts that failed at the 16k limit, 9 were replies cut off before a finished program existed, and all 9 passed when run again with more room. Only Inkling's 5 are wrong programs.

## Two perfect scores

MiniMax M3.1 Flash and DeepSeek V4.1 Flash passed all 30 attempts each. That includes the tasks the others stumbled on: the Kafka-style logs under 1,000 requests a second, the counter that must stay exact through a split, and transactions that must stay available while the two nodes can't talk.

Two habits set them apart, though with 30 attempts each these are patterns, not proof:

- **They tested more.** The median attempt ran Maelstrom 3 times for MiniMax and 3.5 times for DeepSeek V4.1 Flash, against 2 for Inkling and DeepSeek V4 Pro. A bug that shows up one run in three is easier to catch the more you run.
- **They read the grader.** In 25 of 30 attempts for MiniMax and 20 of 30 for DeepSeek V4.1 Flash, the agent opened Maelstrom's jar and read the Clojure source of the checker it would be graded by. Inkling and DeepSeek V4 Pro did this in 12. The next section tests whether that explains their scores; it doesn't.

Their code also avoids the bugs that sank Inkling. In the counter variant, every attempt from both models re-sends its state on a timer, so an update lost to a network split is sent again later. In multi-node broadcast, every attempt guards its shared state with a mutex.

## Does reading the grader explain the perfect scores?

The container has no network, and Maelstrom is installed for the agent to test with, so reading its checker is fair play. But the two models that read it most were also the two that never failed, so it's fair to ask whether one explains the other. Two tests say no.

**Attempts that never looked passed just as often.** Splitting every model's scored attempts by whether the agent opened Maelstrom's jar or a Clojure source file (`python -m distsys_eval.grader_reading results/*/`), and leaving out attempts cut off before their first step:

| model | read the grader | didn't |
|---|---|---|
| MiniMax M3.1 Flash | 25 / 25 | 5 / 5 |
| DeepSeek V4.1 Flash | 20 / 20 | 10 / 10 |
| DeepSeek V4 Pro | 11 / 11 | 16 / 16 |
| GLM-5.3 | 13 / 14 | 10 / 10 |
| Inkling | 9 / 12 | 15 / 17 |
| all | 78 / 82 (95%) | 56 / 58 (97%) |

**With the source removed, the perfect scores held.** Both perfect models ran every task again in a sandbox whose Maelstrom jar had no checker source: the 286 Clojure source files whose compiled classes are in the jar were taken out, so Maelstrom loads exactly what it did before. Kafka and transaction runs give the same verdicts with the stripped jar as with the original, and the other tasks' references still pass and their mutants still fail in it. MiniMax passed 29 of 29 scored attempts and DeepSeek V4.1 Flash 30 of 30.

Removing the source didn't stop the agents looking. In 16 of MiniMax's 30 attempts and 11 of DeepSeek V4.1 Flash's, the agent disassembled the checker's compiled classes instead, with `javap` or by loading them in Clojure. So the cleanest comparison is the stripped-sandbox attempts that never touched the jar at all: 26 of 26 passed.

Reading the grader is a habit of thorough agents, not the reason they pass. It is still worth knowing that capable agents go looking for how they'll be graded, and will disassemble it when the source isn't there.

## Cut off before finishing: DeepSeek V4 Pro and GLM-5.3

The harness caps each model reply at 16,000 output tokens, the same for every model. A reasoning model that thinks for longer than that in one reply gets cut off, and the attempt ends there.

That is every one of DeepSeek V4 Pro's 2 failures and GLM-5.3's 7. In 8 of the 9 it happened on the very first reply, before the agent had written a file. GLM-5.3 reasons at its maximum effort unless told otherwise, and on harder tasks its first plan didn't fit. The other 23 GLM-5.3 attempts and 27 DeepSeek V4 Pro attempts all produced programs, and every one of those programs passed.

To separate the limit from the model, the 9 cut-off attempts were run again, once each, with replies of up to 64,000 tokens and nothing else changed. All 9 passed. With them, DeepSeek V4 Pro goes from 27 of 29 to 29 of 29, and GLM-5.3 from 23 of 30 to 30 of 30.

Rerunning only the cut-off attempts is fair here because none of them got as far as a program: 8 ended on the very first reply, and the ninth ended without code that built. They never had a result to replace, so this is like retrying an attempt the harness let down, not giving failed programs a second try. Rerunning every attempt of both models at 64k would be the cleaner experiment, at about the cost of their first run.

The lesson is about the harness, not the models: a reply limit that suits most models silently penalizes one that reasons at length. The harness should set the limit from each model's own maximum, or report cut-off attempts separately from wrong ones, as here.

## Inkling: right most of the time, wrong in the classic ways

Inkling passed 24 of its 29 scored attempts and finished every task inside its time budget. It is the only model whose failures are wrong programs, and each one tells you something.

**An update sent once and never retried** (g-counter-variant, `fails-under-faults`). Inkling built the counter as a CRDT: each node keeps its own count and sends its whole state to the others whenever it changes. But it sends that state only once, with fire-and-forget `Send`, at the moment of an increment. There is no periodic re-send. If the network is split at that moment, the update never arrives, and the final count stays wrong forever. Inkling's own test runs passed, and it stopped. The grader ran the test twice: the first run was valid, the second wasn't. This is exactly the failure that running the test m times exists to catch.

**A map with no lock** (broadcast-multi-node, `crashed`). The Maelstrom Go library runs message handlers concurrently. Inkling's broadcast node stored seen values in a plain Go map that every handler wrote to, with no mutex. Go stops a program that writes a map from two goroutines at once, the likely cause of the crash. The agent's own test passed; the grader's run ended with a crashed node. The other attempt at the same task used a mutex and passed.

**Wrong on a healthy network** (kafka-single-node, `fails-without-faults`). A single-node log with a mutex-protected map passed the agent's own full-size test, then produced an invalid history in the grader's first run. The other attempt passed.

**A crash under load** (kafka-multi-node-variant, `crashed`). This attempt stored each log as a JSON array in Maelstrom's `lin-kv` store, rewriting the whole array on every append. Its first grading run was valid, its second crashed. Along the way Maelstrom's checker ran out of memory, and Inkling raised its Java heap to 16 GB to get the test through.

**A tool call written as text** (txn-read-committed, `doesnt-build`). Three steps in, Inkling wrote its next tool call as plain text (`<function>read_file</function>…`) instead of calling the tool. The harness took that as the agent's final answer, and the attempt ended with no program.

The common thread: **every program that failed had passed its author's own test.** Each of these bugs shows up on some runs and not others, and Inkling's runs happened to miss it before it declared the program done.

## Variants: no sign of memorization

Four tasks have a variant: the same problem with renamed messages and fields and a different cluster size. A model that remembered the public solutions would do worse on the variants. MiniMax and DeepSeek V4.1 Flash scored the same on all four pairs. The other three each lost points on a variant, but every one of those losses is a cut-off reply or one of Inkling's bugs above, not a sign the model leaned on the original's wording.

## What this says about the benchmark

With four models at 1.00, these 15 tasks no longer separate the best open models. They still separate models that write correct code from ones that don't, and they catch real bugs that pass a single test run. The next version needs harder tasks: more nodes, nastier faults, and problems without a public reference solution.

## Caveats

- **2 attempts per task.** pass^2 is the most this supports. One attempt moves a task's pass^1 by 0.5.
- **Models as Together serves them.** Every model was called through Together's API. Its serving setup, such as quantization, isn't something this run controls.
- **The output limit.** As above, 9 attempts were cut off at the 16,000-token reply limit and run again at 64,000. Both sets of numbers are reported, and the 16k results are kept unchanged in `results/`.
- **The ablation's kafka tasks weren't validated in CI.** The stripped sandbox's other tasks passed `distsys-eval validate` on GitHub's runners. The kafka checker can't reach a verdict there during validation, in the stripped and the normal image alike, so the kafka tasks were validated locally instead, with the same verdicts on both jars. One MiniMax kafka-single-node attempt in that sandbox is unscored for the same reason.
- **2 attempts not scored.** One DeepSeek V4 Pro attempt lost its connection to Together mid-reply on every retry. One Inkling attempt at kafka-multi-node-variant left Maelstrom unable to reach a verdict, most likely because its checker ran out of memory on the runner. Both count as harness errors, not failures.
- **An earlier run on CPUs.** Four 4B–9B models were first run in Ollama on the CI runner's CPU, with no GPU. At 100 to 190 seconds a step they ran out of time on almost every attempt, so those results measured the hardware, not the models, and they aren't reported here.

## Reproducing it

Every attempt's results line, transcript and final code are in [`results/`](results/), one directory per model, and `results/report.txt` has the full per-task tables. To run one model at one task:

```bash
TOGETHER_API_KEY=... distsys-eval run --provider together --model MiniMaxAI/MiniMax-M3.1-Flash --attempts 2 echo
```

To run a set of models in CI, list them in `.github/eval-matrix.json` and add the `run-eval` label to a pull request.
