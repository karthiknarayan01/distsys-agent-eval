package main

import (
	"encoding/json"
	"log"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	node.Handle("init", func(ctx *maelstrom.Context) error {
		id, err := ctx.ID()
		if err != nil {
			return err
		}
		log.Printf("Node initialized with ID: %s", id)

		// Create channels for incoming messages
		broadcastCh := make(chan maelstrom.Message, 256)
		readCh := make(chan maelstrom.Message, 256)
		topologyCh := make(chan map[string][]string, 1)

		// Set up readers in background goroutines
		go func() {
			for {
				msg, err := ctx.Read()
				if err != nil {
					log.Println("Read error:", err)
					continue
				}

				switch msg.Type {
				case "broadcast":
					broadcastCh <- msg
				case "read":
					readCh <- msg
				case "topology":
					topology, ok := msg.Body.(map[string][]string)
					if !ok {
						log.Println("Invalid topology body")
						continue
					}
					topologyCh <- topology
					log.Printf("Node %s got topology", id)
				}
			}
		}()

		// Store node info
		nodeCtx := maelstrom.NodeContext{
			ID:          id,
			BroadcastCh: broadcastCh,
			ReadCh:      readCh,
			TopologyCh:  topologyCh,
		}

		return nil
	})

	node.Handle("broadcast", func(ctx *maelstrom.Context, msg maelstrom.Message) error {
		id, err := ctx.ID()
		if err != nil {
			return err
		}

		body := msg.Body.(map[string]interface{})
		value, _ := body["value"].(float64)
		msgID := int(body["msg_id"].(float64))

		// Forward to all neighbors if we have topology
		topology, _ := ctx.Get("topology").(map[string][]string)
		if topo, ok := topology[id]; ok {
			for _, target := range topo {
				// Convert float64 to int and send via RPC
				neighbor, _ := maelstrom.RPC(ctx, id, "broadcast", map[string]interface{}{
					"value":  value,
					"msg_id": msgID,
				})
				_ = neighbor
			}
		}

		// Store locally with a unique key
		key := broadcastChKey(id, msgID) // This won't work, need different approach
		
		reply(ctx, "broadcast_ok", msg.Body.(map[string]interface{})["msg_id"], nil)
		return nil
	})

	node.Handle("read", func(ctx *maelstrom.Context, msg maelstrom.Message) error {
		id, err := ctx.ID()
		if err != nil {
			return err
		}

		// Collect all values from neighbors and local storage
		values := collectValues(ctx, id)

		reply(ctx, "read_ok", -1, map[string]interface{}{
			"messages": values,
		})
		return nil
	})

	node.Handle("topology_ok", func(ctx *maelstrom.Context, msg maelstrom.Message) error {
		id, _ := ctx.ID()
		body := msg.Body.(map[string]interface{})
		
		topology := make(map[string][]string)
		if topo, ok := body["topology"]; ok {
			for k, v := range topo.(map[string]any) {
				topology[k] = v.([]interface{}) // need proper conversion
			}
		}
		
		ctx.Set("topology", topology)
		log.Printf("Node %s stored topology: %+v", id, topology[id])
		return nil
	})

	node.Run()
}

func reply(ctx *maelstrom.Context, typ string, msgID float64, body interface{}) {
	replyMsg := map[string]interface{}{
		"type":       typ,
		"in_reply_to": msgID,
		"body":       body,
	}
	ctx.Send(maelstrom.Node{}, "out", replyMsg)
}

func broadcastChKey(id string, msgID int) string {
	return id + ":" + keyify(msgID)
}
