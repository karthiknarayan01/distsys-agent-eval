package main

import (
    "fmt"
    m "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    state := m.NewState("test")
    
    // Handle init message
    initMsg, err := state.GetMessage(m.InitMsg{})
    if err != nil {
        fmt.Printf("init error: %v\n", err)
        return
    }
    
    initVal := initMsg.Value.(*m.InitValue)
    fmt.Printf("My ID: %s, Nodes: %v\n", initVal.NodeId, initVal.NodeIds)
}
