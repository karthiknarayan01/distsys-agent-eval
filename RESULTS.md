# Can open models write distributed systems code that survives network faults?

*Results from November 2026. The numbers are in the [README](README.md#-results-november-2026); this page explains them.*

Five open-weights models each got 15 distributed systems tasks, from an echo server to read-committed transactions across a network split. Each wrote a Go program alone in a container. Jepsen's Maelstrom then ran the program as a cluster, injected faults, and checked every answer. Each model had 2 attempts per task.

| model | ran on | pass^1 | pass^2 |
|---|---|---|---|
| Inkling (975B MoE, 41B active) | Together AI | 0.80 | 0.71 |
| ornith-1.5 9B | 4-vCPU runner, no GPU | 0.07 | 0.07 |
| qwen3.5 9B | 4-vCPU runner, no GPU | 0.00 | 0.00 |
| qwen3.5 4B | 4-vCPU runner, no GPU | 0.00 | 0.00 |
| mimo-v2.6 9B | 4-vCPU runner, no GPU | 0.00 | 0.00 |

The short answer: a large open model can write this code, but not reliably. The small models, run on a CPU, never got far enough to show.

## Inkling: right most of the time, wrong in the classic ways

Inkling passed 24 of its 29 scored attempts. It finished every task well inside its time budget, the longest in 12 minutes against limits of 15 to 60.

| task | passed | task | passed |
|---|---|---|---|
| echo | 2/2 | g-counter | 2/2 |
| unique-ids | 2/2 | g-counter-variant | 1/2 |
| unique-ids-variant | 2/2 | kafka-single-node | 1/2 |
| broadcast-single-node | 2/2 | kafka-multi-node | 2/2 |
| broadcast-multi-node | 1/2 | kafka-multi-node-variant | 0/1 |
| broadcast-fault-tolerant | 2/2 | txn-read-committed | 1/2 |
| broadcast-fault-tolerant-variant | 2/2 | | |
| broadcast-few-messages | 2/2 | | |
| broadcast-low-latency | 2/2 | | |

Each of the 5 failures tells you something.

**An update sent once and never retried** (g-counter-variant, `fails-under-faults`). Inkling built the counter as a CRDT: each node keeps its own count and sends its whole state to the others whenever it changes. But it sends that state only once, with fire-and-forget `Send`, at the moment of an increment. There is no periodic re-send. If the network is split at that moment, the update never arrives, and the final count stays wrong forever. Inkling's own test runs passed, and it stopped. The grader ran the test twice: the first run was valid, the second wasn't. This is exactly the failure m runs exist to catch.

**A map with no lock** (broadcast-multi-node, `crashed`). The Maelstrom Go library runs message handlers concurrently. Inkling's broadcast node stored seen values in a plain Go map that every handler wrote to, with no mutex. Go kills a program that writes a map from two goroutines at once. The agent's own test passed. The grader's run ended with a crashed node. The other attempt at the same task used a mutex and passed.

**Wrong on a healthy network** (kafka-single-node, `fails-without-faults`). A single-node log with a mutex-protected map passed the agent's own full-size test, then produced an invalid history in the grader's first run. The other attempt passed.

**A crash under load** (kafka-multi-node-variant, `crashed`). This attempt stored each log as a JSON array in Maelstrom's `lin-kv` store, rewriting the whole array on every append. Its first grading run was valid, its second crashed. Along the way Maelstrom's checker ran out of memory, and Inkling raised its Java heap to 16 GB to get the test through.

**A tool call written as text** (txn-read-committed, `doesnt-build`). Three steps in, Inkling wrote its next tool call as plain text (`<function>read_file</function>…`) instead of calling the tool. The harness took that as the agent's final answer, and the attempt ended with no program. That's a tool-calling slip, not a design mistake, but an agent has to get it right too.

The common thread: **every program that failed had passed its author's own test.** Each of these bugs shows up on some runs and not others, and Inkling's runs happened to miss it before it declared the program done. The grader runs it m times, enough to catch each task's known bugs 99% of the time.

### It read the grader's source

In 12 of 29 attempts, Inkling unzipped Maelstrom's jar and read the Clojure source of the checker it would be graded by, to learn exactly what it checks. The container has no network, so this is fair play: the test tool is installed for the agent to use. But it's worth knowing that a capable agent goes looking for the grader, and that the tasks hold up when it finds it.

### Variants: a hint of memorization

Four tasks have a variant: the same problem with renamed messages and fields and a different cluster size. Inkling passed g-counter and kafka-multi-node every time, but their variants only 1 in 2 and 0 in 1. The other two pairs (unique-ids, broadcast-fault-tolerant) scored the same. Two attempts is too few to call this memorization, but it's the direction you'd expect if it were.

## The small models: out of time before they started

The four small models passed 2 of 120 attempts between them, both from ornith-1.5 at echo. 115 of the rest ended `out-of-time`. Two things compounded:

**They were slow.** On a 4-vCPU runner with no GPU, a 9B model wrote at a few tokens a second. The median step took 100 to 190 seconds, against 7 for Inkling. In the 15 to 60 minutes a task allows, they averaged 13 to 23 steps. Inkling needed about 24 to finish a task.

**They didn't know the library.** The tasks use Maelstrom's Go library, and the small models guessed at its API. qwen3.5 9B's first echo program called `maelstrom.Body` and `maelstrom.HANDLE_COMPLETE`, neither of which exists. In another task it expected `NewNode` to return two values. They then spent most of their steps reading `go doc` output, one type at a time, three minutes per step. Only 4 of their 120 attempts left a program that built.

So these rows say: a 4B to 9B model on a laptop-class CPU can't solve these tasks in the time limit. They don't say whether the same models would on a GPU. That's the obvious next run.

## Caveats

- **2 attempts per task.** pass^2 is the most this supports. A difference of one attempt moves a task's pass^1 by 0.5.
- **The local models' hardware.** See above. Their scores mostly measure the runner.
- **Inkling as Together serves it.** The model was called through Together's API. Its serving setup, such as quantization, isn't something this run controls.
- **The kafka checker on small machines.** Maelstrom's kafka checker sometimes couldn't reach a verdict on these runners, most likely because it ran out of memory. Those runs count as harness errors, not failures. One Inkling attempt at kafka-multi-node-variant stayed undecided after retries and isn't scored, which leaves Inkling with 29 scored attempts.

## Reproducing it

Every attempt's results line, transcript and final code are in [`results/`](results/), one directory per model, and `results/report.txt` has the full per-task tables. The runs used GitHub Actions on commits `2cc8bfb`, `f12ead8` and `2d4d298`, which change only the harness. To rerun one model at one task locally:

```bash
distsys-eval run --provider ollama --model qwen3.5:9b --attempts 2 echo
```
