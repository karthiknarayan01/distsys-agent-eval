package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type addRequest struct {
	MsgID  int        `json:"msg_id"`
	Delta  int64      `json:"delta"`
	TTL    time.Duration
}

type readRequest struct {
	MsgID int `json:"msg_id"`
}

func main() {
	node := maelstrom.NewNode()

	var nids []string
	node.Handle("init", func(m maelstrom.Message) error {
		b, _ := json.Marshal(m.Body)
		switch m.Type {
		case "init":
			body := m.Body
			if body == nil {
				return nil
			}
			bodyArr := []json.RawMessage{}
			json.Unmarshal(body, &bodyArr)
			for _, item := range bodyArr {
				var data map[string]interface{}
				if err := json.Unmarshal(item, &data); err != nil {
					return nil
				}
				nodeID, ok := data["node_id"].(string)
				if !ok {
					continue
				}
				nids = append(nids, nodeID)
			}
		case "init_done":
			log.Printf("Init done: %d nodes", len(nids))
		default:
		}
		return nil
	})

	seqKV := maelstrom.NewSeqKV(node, "seq-kv")

	msgChan := make(chan *maelstrom.Message)
	node.Messages(msgChan)

	for {
		select {
		case msg := <-msgChan:
			switch msg.Type {
			case "add":
				body := msg.Body
				if body == nil {
					break
				}
				var addReq addRequest
				json.Unmarshal(body, &addReq)

				currVal, _ := seqKV.ReadInt([]byte{0}, time.Second)
				newVal := currVal + addReq.Delta

				seqKV.Write([]byte{0}, []byte(fmt.Sprintf("%d", newVal)), time.Second)

				reply := node.Reply()
				json.Unmarshal(addOKResponse{
					MsgID:     addReq.MsgID,
					InReplyTo:  0,
				}, reply.Body)
				node.Send(reply)

			case "read":
				body := msg.Body
				if body == nil {
					break
				}
				var readReq readRequest
				json.Unmarshal(body, &readReq)

				val, _, _ := seqKV.ReadInt([]byte{0}, time.Second)

				reply := node.Reply()
				json.Unmarshal(readOKResponse{
					MsgID:     readReq.MsgID,
					InReplyTo:  0,
					Value:      val,
				}, reply.Body)
				node.Send(reply)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case <-msgChan.Done():
			log.Printf("Messages done")
			return
		}
	}

	seqKV.Close()
	node.Close()
}

type addOKResponse struct {
	MsgID     int    `json:"msg_id"`
	InReplyTo  int    `json:"in_reply_to"`
	AckCount int64 `json:"ack_count,omitempty"`
}

type readOKResponse struct {
	MsgID   int    `json:"msg_id"`
	InReplyTo  int    `json:"in_reply_to"`
	Value     int64 `json:"value"`
	TTL       time.Duration
}
