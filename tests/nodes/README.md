Tiny Maelstrom nodes for the harness's own tests: an echo node that's right and one that
replies with the wrong text, in Go (built in the sandbox image) and in Python (run directly
by the host tests), and a Go echo node that speaks a renamed protocol (`say`, `words` and
`said`), for testing maelstrom-rename.
