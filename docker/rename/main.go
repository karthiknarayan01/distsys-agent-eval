// Command maelstrom-rename sits between Maelstrom and a node, so a task can give its protocol
// names of its own. Maelstrom starts it as the node binary; it starts the real node and passes
// every message through, renaming message types and body fields on the way.
//
//	MAELSTROM_RENAME_BIN=/work/node \
//	MAELSTROM_RENAME='{"types": {"echo": "say", "echo_ok": "said"}, "fields": {"echo": "words"}}' \
//	maelstrom test --workload echo --bin /usr/local/bin/maelstrom-rename ...
//
// The maps go from Maelstrom's names to the task's. Only messages between a client and a node
// are renamed: a client's request on its way in, and the node's reply on its way out. Messages
// between nodes, and to and from Maelstrom's services such as seq-kv, pass through as they are,
// so the Go library's own requests to those services keep working. Within a renamed message,
// the body's type and its top-level keys are renamed; values are left alone. A line that isn't
// a message passes through unchanged, and stderr goes straight to Maelstrom's log.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
)

// maxLine is the longest message line we pass on. Maelstrom's own messages are far shorter,
// but a node may batch a lot into one message to another node.
const maxLine = 64 << 20

func main() {
	toNode, err := loadRenames(os.Getenv("MAELSTROM_RENAME"))
	if err != nil {
		fail("MAELSTROM_RENAME: %v", err)
	}
	binary := os.Getenv("MAELSTROM_RENAME_BIN")
	if binary == "" {
		fail("MAELSTROM_RENAME_BIN is not set")
	}
	os.Exit(run(binary, toNode, toNode.inverse(), os.Stdin, os.Stdout))
}

// run starts the node and relays both directions until it exits, then returns its exit code.
func run(binary string, toNode, fromNode renames, in io.Reader, out io.Writer) int {
	node := exec.Command(binary)
	node.Stderr = os.Stderr
	nodeIn, err := node.StdinPipe()
	if err != nil {
		fail("%v", err)
	}
	nodeOut, err := node.StdoutPipe()
	if err != nil {
		fail("%v", err)
	}
	if err := node.Start(); err != nil {
		fail("starting %s: %v", binary, err)
	}

	go func() {
		relay(in, nodeIn, fromClient, toNode)
		nodeIn.Close()
	}()
	relay(nodeOut, out, toClient, fromNode)

	if err := node.Wait(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ExitCode()
		}
		return 1
	}
	return 0
}

// relay copies lines from in to out, renaming the messages that picked selects.
func relay(in io.Reader, out io.Writer, picked func(from, to string) bool, names renames) {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64<<10), maxLine)
	writer := bufio.NewWriter(out)
	for scanner.Scan() {
		writer.Write(translate(scanner.Bytes(), picked, names))
		writer.WriteByte('\n')
		writer.Flush()
	}
}

// translate renames one line if it is a message that picked selects; anything else comes back
// as it was. Only the body changes: the rest of the message is copied untouched.
func translate(line []byte, picked func(from, to string) bool, names renames) []byte {
	var envelope map[string]json.RawMessage
	var body map[string]json.RawMessage
	var from, to string
	if json.Unmarshal(line, &envelope) != nil || json.Unmarshal(envelope["body"], &body) != nil ||
		json.Unmarshal(envelope["src"], &from) != nil || json.Unmarshal(envelope["dest"], &to) != nil ||
		!picked(from, to) {
		return line
	}
	renamed := map[string]json.RawMessage{}
	for key, value := range body {
		if name, ok := names.Fields[key]; ok {
			key = name
		}
		renamed[key] = value
	}
	var kind string
	if json.Unmarshal(renamed["type"], &kind) == nil {
		if name, ok := names.Types[kind]; ok {
			renamed["type"], _ = json.Marshal(name)
		}
	}
	envelope["body"], _ = json.Marshal(renamed)
	out, err := json.Marshal(envelope)
	if err != nil {
		return line
	}
	return out
}

// clientID matches Maelstrom's client ids: c1, c2, ...
var clientID = regexp.MustCompile(`^c[0-9]+$`)

// fromClient picks a client's messages to a node; toClient, a node's replies to a client.
func fromClient(from, _ string) bool { return clientID.MatchString(from) }
func toClient(_, to string) bool     { return clientID.MatchString(to) }

// renames maps old names to new ones, for message types and for body fields.
type renames struct {
	Types  map[string]string `json:"types"`
	Fields map[string]string `json:"fields"`
}

// loadRenames reads the maps from their JSON. Each must be one to one, or a reply couldn't be
// renamed back.
func loadRenames(text string) (renames, error) {
	var names renames
	if err := json.Unmarshal([]byte(text), &names); err != nil {
		return names, err
	}
	for what, table := range map[string]map[string]string{"types": names.Types, "fields": names.Fields} {
		if len(invert(table)) != len(table) {
			return names, fmt.Errorf("two %s are renamed to the same name", what)
		}
	}
	return names, nil
}

// inverse is the renames the other way: from the task's names back to Maelstrom's.
func (names renames) inverse() renames {
	return renames{Types: invert(names.Types), Fields: invert(names.Fields)}
}

func invert(table map[string]string) map[string]string {
	inverted := map[string]string{}
	for old, renamed := range table {
		inverted[renamed] = old
	}
	return inverted
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "maelstrom-rename: "+format+"\n", args...)
	os.Exit(2)
}
