# Your workspace

You are in `/work`, inside a Linux container with no network access. Write a Go program here.
[Maelstrom](https://github.com/jepsen-io/maelstrom) will start that program once for every node
of a simulated cluster, send it client requests, and check that the answers are correct.

What is installed:

- Go 1.27.
- Maelstrom 0.2.4, as the `maelstrom` command, so you can test your program yourself.
- Maelstrom's Go library, already in the module cache. Start your module with:

      go mod init node
      go get github.com/jepsen-io/maelstrom/demo/go@v0.0.0-20260710121306-480a8197026d

A node talks to the rest of the cluster through standard input and output: it reads one JSON
message per line on stdin and writes one per line on stdout. Each message has a `src`, a `dest`
and a `body`, and the body's `type` says what it is. Anything written to stderr is a log line.
The library does that framing for you: `maelstrom.NewNode()`, then `Handle` a message type,
`Reply` to a request, `Send` or `RPC` to another node, and `Run` to start. It also answers the
`init` message that tells each node its own ID and the IDs of all the nodes.
