package main

import (
	"context"
	"fmt"
	
	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	fmt.Println(dir, "NewSeqKV:", go.NewSeqKV(nil))
}
