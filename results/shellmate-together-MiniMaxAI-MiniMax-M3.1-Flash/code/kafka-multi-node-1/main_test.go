package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type wireMessage struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
}

type wireBody struct {
	Type      string         `json:"type"`
	MsgID     int            `json:"msg_id"`
	InReplyTo int            `json:"in_reply_to"`
	Key       string         `json:"key"`
	From      any            `json:"from"`
	To        any            `json:"to"`
	Value     any            `json:"value"`
	Offset    int            `json:"offset"`
	Offsets   map[string]int `json:"offsets"`
	Keys      []string       `json:"keys"`
	Create    bool           `json:"create_if_not_exists"`
	Code      int            `json:"code"`
	Text      string         `json:"text"`
}

type fakeKV struct {
	mu   sync.Mutex
	data map[string]any
}

func (f *fakeKV) handle(t *testing.T, in io.Reader, out io.Writer, clientOut io.Writer) {
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		var m wireMessage
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Errorf("decode node message: %v", err)
			return
		}
		var b wireBody
		if err := json.Unmarshal(m.Body, &b); err != nil {
			t.Errorf("decode body: %v", err)
			return
		}
		if b.Type != "read" && b.Type != "cas" {
			// This is a client reply; pass it to the test client.
			clientOut.Write(append(sc.Bytes(), '\n'))
			continue
		}
		f.mu.Lock()
		cur, exists := f.data[b.Key]
		var resp wireBody
		switch b.Type {
		case "read":
			if !exists {
				resp = wireBody{Type: "error", Code: 20, Text: "key does not exist"}
			} else {
				resp = wireBody{Type: "read_ok", Value: cur}
			}
		case "cas":
			if exists && !reflect.DeepEqual(cur, b.From) {
				resp = wireBody{Type: "error", Code: 22, Text: "precondition failed"}
			} else if !exists && b.From != nil {
				resp = wireBody{Type: "error", Code: 22, Text: "precondition failed"}
			} else {
				f.data[b.Key] = b.To
				resp = wireBody{Type: "cas_ok"}
			}
		}
		f.mu.Unlock()
		resp.InReplyTo = b.MsgID
		raw, _ := json.Marshal(resp)
		wire, _ := json.Marshal(wireMessage{Src: "lin-kv", Dest: "n0", Body: raw})
		out.Write(append(wire, '\n'))
	}
}

func TestEndToEnd(t *testing.T) {
	node := maelstrom.NewNode()
	node.Init("n0", []string{"n0"})
	kv := maelstrom.NewLinKV(node)
	svc := newService(node, kv)
	node.Handle("send", svc.handleSend)
	node.Handle("poll", svc.handlePoll)
	node.Handle("commit_offsets", svc.handleCommitOffsets)
	node.Handle("list_committed_offsets", svc.handleListCommittedOffsets)

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	clientR, clientW := io.Pipe()
	node.Stdin, node.Stdout = inR, outW
	f := &fakeKV{data: map[string]any{}}
	go f.handle(t, outR, inW, clientW)
	done := make(chan error, 1)
	go func() { done <- node.Run() }()

	clientScanner := bufio.NewScanner(clientR)
	sendReq := func(id int, body map[string]any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(body)
		fmt.Fprintf(inW, `{"src":"c0","dest":"n0","body":%s}`+"\n", raw)
		sc := clientScanner
		for sc.Scan() {
			var m wireMessage
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				t.Fatal(err)
			}
			var b wireBody
			if err := json.Unmarshal(m.Body, &b); err != nil {
				t.Fatal(err)
			}
			if b.InReplyTo == id {
				return map[string]any{"type": b.Type, "value": b.Value, "offset": b.Offset, "offsets": b.Offsets, "raw": string(m.Body)}
			}
		}
		t.Fatal("no reply")
		return nil
	}

	r := sendReq(1, map[string]any{"type": "send", "msg_id": 1, "key": "k", "msg": 123})
	if r["type"] != "send_ok" || r["offset"] != 0 {
		t.Fatalf("bad send reply: %#v", r)
	}
	r = sendReq(2, map[string]any{"type": "send", "msg_id": 2, "key": "k", "msg": 456})
	if r["type"] != "send_ok" || r["offset"] != 1 {
		t.Fatalf("bad send reply: %#v", r)
	}
	r = sendReq(3, map[string]any{"type": "poll", "msg_id": 3, "offsets": map[string]int{"k": 0}})
	if r["type"] != "poll_ok" {
		t.Fatalf("bad poll reply: %#v", r)
	}
	r = sendReq(7, map[string]any{"type": "send", "msg_id": 7, "key": "v", "msg": json.RawMessage(`{"big":12345678901234567890,"s":"x"}`)})
	if r["type"] != "send_ok" {
		t.Fatalf("bad send reply: %#v", r)
	}
	r = sendReq(8, map[string]any{"type": "poll", "msg_id": 8, "offsets": map[string]int{"v": 0}})
	if r["type"] != "poll_ok" || !strings.Contains(r["raw"].(string), "12345678901234567890") {
		t.Fatalf("poll did not preserve value exactly: %#v", r)
	}
	r = sendReq(4, map[string]any{"type": "commit_offsets", "msg_id": 4, "offsets": map[string]int{"k": 5}})
	if r["type"] != "commit_offsets_ok" {
		t.Fatalf("bad commit reply: %#v", r)
	}
	r = sendReq(5, map[string]any{"type": "commit_offsets", "msg_id": 5, "offsets": map[string]int{"k": 2}})
	if r["type"] != "commit_offsets_ok" {
		t.Fatalf("bad commit reply: %#v", r)
	}
	r = sendReq(6, map[string]any{"type": "list_committed_offsets", "msg_id": 6, "keys": []string{"k", "missing"}})
	if r["type"] != "list_committed_offsets_ok" {
		t.Fatalf("bad list reply: %#v", r)
	}
	offs, ok := r["offsets"].(map[string]int)
	if !ok || offs["k"] != 5 || len(offs) != 1 {
		t.Fatalf("bad offsets: %#v", r["offsets"])
	}

	inW.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("node did not stop")
	}
}
