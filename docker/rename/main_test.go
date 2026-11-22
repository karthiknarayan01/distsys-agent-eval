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

func TestARequestToAServiceIsAnsweredWithAnError(t *testing.T) {
	line := []byte(`{"src":"n1","dest":"lin-kv","body":{"type":"read","msg_id":7,"key":"x"}}`)
	got := decode(t, serviceError(line))
	want := decode(t, []byte(`{"src":"lin-kv","dest":"n1","body":{"type":"error","in_reply_to":7,"code":10,"text":"lin-kv is turned off for this task"}}`))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestMessagesToNodesAndClientsAreNotTurnedAway(t *testing.T) {
	for _, line := range []string{
		`{"src":"n1","dest":"n2","body":{"type":"append_entries","msg_id":3}}`,
		`{"src":"n1","dest":"c4","body":{"type":"read_ok","in_reply_to":1,"value":3}}`,
		`not a message`,
	} {
		if answer := serviceError([]byte(line)); answer != nil {
			t.Fatalf("%s was turned away: %s", line, answer)
		}
	}
}

func TestTheRelayTurnsAwayServiceRequestsAndPassesTheRest(t *testing.T) {
	in := strings.NewReader(`{"src":"n1","dest":"seq-kv","body":{"type":"read","msg_id":1,"key":"k"}}` + "\n" +
		`{"src":"n1","dest":"n2","body":{"type":"gossip","msg_id":2}}` + "\n")
	var out, back strings.Builder
	relay(in, &out, toClient, renames{}, serviceError, &back)
	if !strings.Contains(out.String(), `"dest":"n2"`) || strings.Contains(out.String(), "seq-kv") {
		t.Fatalf("passed on: %s", out.String())
	}
	if !strings.Contains(back.String(), `"in_reply_to":1`) {
		t.Fatalf("answered: %s", back.String())
	}
}

func TestNoRenamesIsAllowed(t *testing.T) {
	names, err := loadRenames("")
	if err != nil || len(names.Types) != 0 || len(names.Fields) != 0 {
		t.Fatalf("got %v, %v", names, err)
	}
}
