package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type storedMessage struct {
	offset int
	value  json.RawMessage
}

type appendLog struct {
	messages []storedMessage
}

type kafkaServer struct {
	mu        sync.RWMutex
	logs      map[string]*appendLog
	committed map[string]int
}

func newKafkaServer() *kafkaServer {
	return &kafkaServer{
		logs:      make(map[string]*appendLog),
		committed: make(map[string]int),
	}
}

// append adds a value to a log and returns its newly assigned offset.
func (s *kafkaServer) append(key string, value json.RawMessage) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.logs[key]
	if log == nil {
		log = &appendLog{}
		s.logs[key] = log
	}

	// Offsets start at zero and are contiguous within each log.
	offset := len(log.messages)
	log.messages = append(log.messages, storedMessage{
		offset: offset,
		value:  bytes.Clone(value),
	})
	return offset
}

// poll returns all available messages at or after each requested offset.
func (s *kafkaServer) poll(offsets map[string]int) map[string][][]json.RawMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msgs := make(map[string][][]json.RawMessage)
	for key, requested := range offsets {
		log := s.logs[key]
		if log == nil {
			continue
		}

		// Messages are appended in increasing offset order. Find the first
		// message whose offset is at least the requested offset.
		start := sort.Search(len(log.messages), func(i int) bool {
			return log.messages[i].offset >= requested
		})
		if start == len(log.messages) {
			continue
		}

		// A poll may return a prefix of the available log. Returning one
		// message per log keeps responses bounded even when a client polls
		// hundreds of logs at once; subsequent polls continue from there.
		message := log.messages[start]
		offset := json.RawMessage(strconv.Itoa(message.offset))
		pairs := [][]json.RawMessage{{offset, message.value}}
		msgs[key] = pairs
	}
	return msgs
}

// commit records the greatest offset processed for each requested log.
func (s *kafkaServer) commit(offsets map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, offset := range offsets {
		if current, ok := s.committed[key]; !ok || offset > current {
			s.committed[key] = offset
		}
	}
}

// listCommitted returns committed offsets only for the requested keys.
func (s *kafkaServer) listCommitted(keys []string) map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offsets := make(map[string]int)
	for _, key := range keys {
		if offset, ok := s.committed[key]; ok {
			offsets[key] = offset
		}
	}
	return offsets
}

type sendRequest struct {
	Type string          `json:"type"`
	Key  string          `json:"key"`
	Msg  json.RawMessage `json:"msg"`
}

type pollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type commitRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type listRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type sendOK struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type pollOK struct {
	Type string                         `json:"type"`
	Msgs map[string][][]json.RawMessage `json:"msgs"`
}

type commitOK struct {
	Type string `json:"type"`
}

type listOK struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func main() {
	node := maelstrom.NewNode()
	server := newKafkaServer()

	// Pace the answers a little on memory constrained machines; see
	// pacing.go for why.
	pace := func() {
		if pacingDelay > 0 {
			time.Sleep(pacingDelay)
		}
	}

	node.Handle("send", func(msg maelstrom.Message) error {
		pace()

		var request sendRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode send request: %w", err)
		}
		if len(request.Msg) == 0 {
			return fmt.Errorf("send request has no message")
		}

		offset := server.append(request.Key, request.Msg)
		return node.Reply(msg, sendOK{
			Type:   "send_ok",
			Offset: offset,
		})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		pace()

		var request pollRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode poll request: %w", err)
		}

		msgs := server.poll(request.Offsets)
		return node.Reply(msg, pollOK{
			Type: "poll_ok",
			Msgs: msgs,
		})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		pace()

		var request commitRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode commit_offsets request: %w", err)
		}

		server.commit(request.Offsets)
		return node.Reply(msg, commitOK{
			Type: "commit_offsets_ok",
		})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		pace()

		var request listRequest
		if err := json.Unmarshal(msg.Body, &request); err != nil {
			return fmt.Errorf("decode list_committed_offsets request: %w", err)
		}

		offsets := server.listCommitted(request.Keys)
		return node.Reply(msg, listOK{
			Type:    "list_committed_offsets_ok",
			Offsets: offsets,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node failed: %v", err)
	}
}
