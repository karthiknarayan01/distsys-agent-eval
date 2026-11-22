package main

import (
	"context"
	
	"github.com/jepsen-io/maelstrom/demo/go"
)

func testKV() {
	node := go.NewNode()
	seqkv := go.NewSeqKV(node)
	ctx := context.Background()
	
	current, _ := seqkv.ReadInt(ctx, "test", 0)
	println("current:", current)
}
