package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	node        *go.Node
	myRumors    map[int]int // count of tells for each rumor
	neighbors   map[string][]string
}

func main() {
	n := &Node{
		node:     go.NewNode(),
		myRumors: make(map[int]int),
	}
	
	for {
		select {
		case <-n.node.InitMsg():
			fmt.Fprintln(os.Stderr, "Node initialized")
			
			if err := n.node.Handle([]byte("init"), nil); err != nil {
				fmt.Fprintln(os.Stderr, "handle init error:", err)
			}
			
			break
			
		case m := <-n.node.Handle():
			bodyBytes, _ := json.Marshal(m.Body)
			var body map[string]interface{}
			json.Unmarshal(bodyBytes, &body)
			
			bodyType := string(body["type"])
			
			switch bodyType {
			case "tell":
				rumorID, _ := body["rumor"].(int)
				n.myRumors[rumorID]++
				
				respBody, _ := json.Marshal(struct{ Rumor int }{Rumor: rumorID})
				n.Reply(m.ID, respBody)
				
			case "recall":
				resp := make([]int, 0, len(n.myRumors))
				for k := range n.myRumors {
					resp = append(resp, k)
				}
				
				respBody, _ := json.Marshal(&resp)
				n.Reply(m.ID, respBody)
			}
			
			time.Sleep(time.Millisecond * 5)
		}
		
		select {}
	}
}
