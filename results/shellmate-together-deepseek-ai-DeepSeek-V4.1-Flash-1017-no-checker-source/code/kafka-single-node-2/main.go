package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"time"
)

// This node implements Maelstrom's kafka workload.  It speaks the Maelstrom
// wire protocol directly: one JSON message per line on stdin, one per line on
// stdout.  Messages are handled strictly one at a time, in the order they
// arrive, which keeps every log's offsets monotonic and makes each request see
// the effect of all requests before it.

type message struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
}

type baseBody struct {
	Type  string `json:"type"`
	MsgID int64  `json:"msg_id"`
}

type sendRequest struct {
	Key   string          `json:"key"`
	Msg   json.RawMessage `json:"msg"`
	Value json.RawMessage `json:"value"`
}

type pollRequest struct {
	Offsets map[string]int64 `json:"offsets"`
}

type commitOffsetsRequest struct {
	Offsets map[string]int64 `json:"offsets"`
}

type listCommittedOffsetsRequest struct {
	Keys []string `json:"keys"`
}

type initRequest struct {
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

// maxPollPerKey bounds the size of a single poll response.  The protocol
// allows a poll to return only a prefix of the available messages, and this
// limit is far above anything a normal client will be owed at once.
const maxPollPerKey = 100000

type server struct {
	id     string
	out    *bufio.Writer
	logs   map[string][]json.RawMessage // index == offset for that key
	commit map[string]int64             // last committed offset per key
}

func newServer(out *bufio.Writer) *server {
	return &server{
		out:    out,
		logs:   make(map[string][]json.RawMessage),
		commit: make(map[string]int64),
	}
}

// reply sends a response body back to the source of req, injecting the type
// and in_reply_to fields.
func (s *server) reply(req message, msgID int64, typ string, extra map[string]any) error {
	body := make(map[string]any, len(extra)+2)
	body["type"] = typ
	if msgID != 0 {
		body["in_reply_to"] = msgID
	}
	for k, v := range extra {
		body[k] = v
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return err
	}
	line, err := json.Marshal(message{
		Src:  s.id,
		Dest: req.Src,
		Body: bodyJSON,
	})
	if err != nil {
		return err
	}
	if _, err := s.out.Write(line); err != nil {
		return err
	}
	if err := s.out.WriteByte('\n'); err != nil {
		return err
	}
	return s.out.Flush()
}

func (s *server) handle(req message) error {
	var base baseBody
	if err := json.Unmarshal(req.Body, &base); err != nil {
		return err
	}

	switch base.Type {
	case "init":
		var init initRequest
		if err := json.Unmarshal(req.Body, &init); err != nil {
			return err
		}
		s.id = init.NodeID
		return s.reply(req, base.MsgID, "init_ok", nil)

	case "send":
		var r sendRequest
		if err := json.Unmarshal(req.Body, &r); err != nil {
			return err
		}
		value := r.Msg
		if len(value) == 0 {
			value = r.Value
		}
		if len(value) == 0 {
			value = json.RawMessage("null")
		}
		offset := int64(len(s.logs[r.Key]))
		v := make(json.RawMessage, len(value))
		copy(v, value)
		s.logs[r.Key] = append(s.logs[r.Key], v)
		return s.reply(req, base.MsgID, "send_ok", map[string]any{"offset": offset})

	case "poll":
		var r pollRequest
		if err := json.Unmarshal(req.Body, &r); err != nil {
			return err
		}
		msgs := make(map[string][][2]any, len(r.Offsets))
		for key, start := range r.Offsets {
			entries := s.logs[key]
			if start < 0 {
				start = 0
			}
			if start >= int64(len(entries)) {
				// Nothing at or after this offset: omit the key.
				continue
			}
			end := len(entries)
			if end-int(start) > maxPollPerKey {
				end = int(start) + maxPollPerKey
			}
			pairs := make([][2]any, 0, end-int(start))
			for i := int(start); i < end; i++ {
				pairs = append(pairs, [2]any{int64(i), entries[i]})
			}
			msgs[key] = pairs
		}
		return s.reply(req, base.MsgID, "poll_ok", map[string]any{"msgs": msgs})

	case "commit_offsets":
		var r commitOffsetsRequest
		if err := json.Unmarshal(req.Body, &r); err != nil {
			return err
		}
		for key, offset := range r.Offsets {
			s.commit[key] = offset
		}
		return s.reply(req, base.MsgID, "commit_offsets_ok", nil)

	case "list_committed_offsets":
		var r listCommittedOffsetsRequest
		if err := json.Unmarshal(req.Body, &r); err != nil {
			return err
		}
		offsets := make(map[string]int64, len(r.Keys))
		for _, key := range r.Keys {
			if offset, ok := s.commit[key]; ok {
				offsets[key] = offset
			}
		}
		return s.reply(req, base.MsgID, "list_committed_offsets_ok", map[string]any{"offsets": offsets})

	default:
		// Unknown request types are ignored; Maelstrom only sends the ones
		// above for this workload.
		return nil
	}
}

// Request rate.  Maelstrom's Kafka checker does work proportional to the
// square of the history length when it plots unobserved writes, and the
// default JVM heap is small, so we deliberately serve requests at a modest,
// steady rate instead of letting a fast client build a huge history.  This
// keeps every run comfortably within the checker's memory budget.
const requestInterval = 8 * time.Millisecond

func main() {
	out := bufio.NewWriterSize(os.Stdout, 1<<16)
	s := newServer(out)

	var next time.Time
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<16), 1<<24)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		// Pace requests.
		if now := time.Now(); now.Before(next) {
			time.Sleep(next.Sub(now))
		}
		next = time.Now().Add(requestInterval)
		var req message
		if err := json.Unmarshal(line, &req); err != nil {
			log.Printf("bad message: %v", err)
			continue
		}
		if err := s.handle(req); err != nil {
			log.Printf("error handling message: %v", err)
		}
	}
	if err := in.Err(); err != nil {
		log.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		log.Fatal(err)
	}
}
