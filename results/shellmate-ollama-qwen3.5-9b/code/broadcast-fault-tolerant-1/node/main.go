package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type values struct {
	Data map[string]struct{} `json:"data"`
}

func main() {
	ctx := context.Background()
	stdout := maelstrom.NewStdoutWriter(ctx, nil)
	node, _ := maelstrom.NewNode(ctx, io.NopCloser(stdout), stdout, nil)

	if err := node.Handle("init", func(c maelstrom.Client) error {
		values = &values{Data: make(map[string]struct{})}
		fmt.Printf("Node %s initialized\n", c.ID())
		return nil
	}); err != nil {
		fmt.Printf("init: %v\n", err)
		return
	}

	if err := node.Handle("topology", func(msg maelstrom.Message, client maelstrom.Client) error {
		var raw struct {
			Topologies map[string][]string `json:"topologies"`
		}
		body, _ := client.Body.Bytes()
		json.Unmarshal(body, &raw)

		topos := raw.Topologies
		nodeId := node.ID()
		
		// Get my neighbors from the topology
		myNeighbors := topos[nodeId]
		
		// Forward values to all neighbors
		for _, neighborID := range myNeighbors {
			if err := client.Send(neighborID, "broadcast_value", msg.Body.Bytes()); err != nil {
				fmt.Printf("send %s broadcast_value: %v\n", neighborID, err)
			}
		}

		// Send updated topology to neighbors (so they can also forward values)
		bodyToSend, _ := json.Marshal(topos)
		for _, neighborID := range myNeighbors {
			if err := client.Send(neighborID, "topology", bodyToSend); err != nil {
				fmt.Printf("send %s topology: %v\n", neighborID, err)
			}
		}

		client.Reply("topology_ok", msg.MsgID, msg.Body.Bytes())
		return nil
	}); err != nil {
		fmt.Printf("topology: %v\n", err)
		return
	}

	if err := node.Handle("broadcast", func(msg maelstrom.Message, client maelstrom.Client) error {
		var raw struct {
			Message int `json:"message"`
		}
		body, _ := client.Body.Bytes()
		json.Unmarshal(body, &raw)

		key := fmt.Sprintf("%d", raw.Message)
		values.Data[key] = struct{}{}

		// Get my neighbors from topology (which was set during init)
		neighbors := []string{"1"} // Placeholder - will be set in real code
		
		for _, neighborID := range neighbors {
			if err := client.Send(neighborID, "broadcast_value", body); err != nil {
				fmt.Printf("send %s broadcast_value: %v\n", neighborID, err)
			}
		}

		client.Reply("broadcast_ok", msg.MsgID, body)
		return nil
	}); err != nil {
		fmt.Printf("broadcast: %v\n", err)
		return
	}

	if err := node.Handle("read", func(msg maelstrom.Message, client maelstrom.Client) error {
		responseValues := make([]json.RawMessage, 0)
		
		for _, key := range values.Data {
			valBytes, _ := json.Marshal(key)
			responseValues = append(responseValues, valBytes)
		}

		response := struct {
			Values []json.RawMessage `json:"values"`
		}{
			Values: responseValues,
		}
		body, _ := json.Marshal(response)
		client.Reply("read_ok", msg.MsgID, body)
		return nil
	}); err != nil {
		fmt.Printf("read: %v\n", err)
		return
	}

	if err := node.Handle("broadcast_value", func(msg maelstrom.Message, client maelstrom.Client) error {
		var raw struct {
			Message int `json:"message"`
		}
		body, _ := client.Body.Bytes()
		json.Unmarshal(body, &raw)

		key := fmt.Sprintf("%d", raw.Message)
		values.Data[key] = struct{}{}

		return nil
	}); err != nil {
		fmt.Printf("broadcast_value: %v\n", err)
		return
	}

	node.Run()
}
