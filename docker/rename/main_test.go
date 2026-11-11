package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

var echo = renames{
	Types:  map[string]string{"echo": "say", "echo_ok": "said"},
	Fields: map[string]string{"echo": "words"},
}

// decode turns a line back into plain values, so tests don't depend on key order.
func decode(t *testing.T, line []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("not JSON: %s", line)
	}
	return m
}

func TestAClientRequestIsRenamedOnTheWayIn(t *testing.T) {
	line := []byte(`{"id":4,"src":"c1","dest":"n0","body":{"type":"echo","msg_id":1,"echo":"hi"}}`)
	got := decode(t, translate(line, fromClient, echo))
	want := decode(t, []byte(`{"id":4,"src":"c1","dest":"n0","body":{"type":"say","msg_id":1,"words":"hi"}}`))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAReplyIsRenamedBackOnTheWayOut(t *testing.T) {
	line := []byte(`{"src":"n0","dest":"c1","body":{"type":"said","in_reply_to":1,"words":"hi"}}`)
	got := decode(t, translate(line, toClient, echo.inverse()))
	want := decode(t, []byte(`{"src":"n0","dest":"c1","body":{"type":"echo_ok","in_reply_to":1,"echo":"hi"}}`))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestMessagesBetweenNodesAndServicesPassThrough(t *testing.T) {
	for _, line := range []string{
		`{"src":"n0","dest":"n1","body":{"type":"echo","echo":"mine"}}`,
		`{"src":"n0","dest":"seq-kv","body":{"type":"read","key":"k"}}`,
	} {
		if got := translate([]byte(line), toClient, echo.inverse()); string(got) != line {
			t.Fatalf("changed %s into %s", line, got)
		}
	}
}

func TestLinesThatArentMessagesPassThrough(t *testing.T) {
	for _, line := range []string{"", "not json", `[1,2]`, `{"src":"c1","dest":"n0","body":5}`} {
		if got := translate([]byte(line), fromClient, echo); string(got) != line {
			t.Fatalf("changed %q into %q", line, got)
		}
	}
}

func TestLargeNumbersKeepEveryDigit(t *testing.T) {
	line := []byte(`{"src":"c1","dest":"n0","body":{"type":"echo","echo":12345678901234567890}}`)
	got := string(translate(line, fromClient, echo))
	if want := `"words":12345678901234567890`; !strings.Contains(got, want) {
		t.Fatalf("%s has no %s", got, want)
	}
}

func TestTwoNamesRenamedToOneAreRefused(t *testing.T) {
	if _, err := loadRenames(`{"types": {"echo": "say", "echo_ok": "say"}}`); err == nil {
		t.Fatal("loaded a map that can't be reversed")
	}
}
