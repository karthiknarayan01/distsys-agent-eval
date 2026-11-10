"""The real sandbox image: it holds the toolchain, and a Maelstrom node builds and passes a
test with no network. Run when SANDBOX_TEST_IMAGE is the built sandbox image; skipped otherwise,
or when the image has no Go."""

import os

import pytest

from distsys_eval.sandbox import Sandbox, SandboxConfig

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE")
pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not IMAGE, reason="set SANDBOX_TEST_IMAGE to the sandbox image to run these"),
]

ECHO_NODE = """\
package main

import (
\t"encoding/json"
\t"log"

\tmaelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
\tn := maelstrom.NewNode()
\tn.Handle("echo", func(msg maelstrom.Message) error {
\t\tvar body map[string]any
\t\tif err := json.Unmarshal(msg.Body, &body); err != nil {
\t\t\treturn err
\t\t}
\t\tbody["type"] = "echo_ok"
\t\treturn n.Reply(msg, body)
\t})
\tif err := n.Run(); err != nil {
\t\tlog.Fatal(err)
\t}
}
"""

GO_MOD = """\
module echo

go 1.27

require github.com/jepsen-io/maelstrom/demo/go v0.0.0-20260710121306-480a8197026d
"""


async def test_an_echo_node_builds_offline_and_passes_maelstrom():
    async with Sandbox.start(SandboxConfig(image=IMAGE)) as box:
        if not (await box.exec("command -v go", timeout=10)).ok:
            pytest.skip(f"{IMAGE} has no Go: not the sandbox image")
        await box.write_file("go.mod", GO_MOD)
        await box.write_file("main.go", ECHO_NODE)
        built = await box.exec("go build -o node .", timeout=300)
        assert built.ok, built.output
        tested = await box.exec(
            "maelstrom test -w echo --bin ./node --node-count 1 --time-limit 5", timeout=300
        )
    assert tested.ok, tested.output
    assert "Everything looks good" in tested.output
