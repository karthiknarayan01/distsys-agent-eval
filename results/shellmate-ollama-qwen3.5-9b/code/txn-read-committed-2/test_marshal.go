package main

import (
	"encoding/json"
	"fmt"
)

func main() {
	body := map[string]interface{}{
		"type": "txn",
		"msg_id": 1,
		"txn": []interface{}{
			map[string]interface{}{"key": 1, "value": nil, "type": "r"},
			map[string]interface{}{"key": 1, "value": 6, "type": "w"},
			map[string]interface{}{"key": 2, "value": 9, "type": "w"},
			map[string]interface{}{"key": 2, "value": nil, "type": "r"},
		},
	}
	b, _ := json.MarshalIndent(body, "", "  ")
	fmt.Println(string(b))
	
	var result map[string]interface{}
	json.Unmarshal(b, &result)
	fmt.Printf("txn field: %v\n", result["txn"])
}
