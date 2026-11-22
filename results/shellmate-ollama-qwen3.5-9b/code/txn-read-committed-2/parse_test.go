package main

import (
	"encoding/json"
)

type MessageBody struct {
	Type        string `json:"type,omitempty"`
	MsgID       int    `json:"msg_id,omitempty"`
	InReplyTo   int    `json:"in_reply_to,omitempty"`
	Code        int    `json:"code,omitempty"`
	Text        string `json:"text,omitempty"`
}

func (m MessageBody) GetInitBody() map[string]interface{} {
	return m
}

func (m MessageBody) GetTxnBody() map[string]interface{} {
	return m
}

type TxnOp struct {
	Key   int      `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
	Type  string   `json:"type,omitempty"`
}

type TxnListBody struct {
	MessageBody
	MsgId    int64             `json:"msg_id,omitempty"`
	Txn      []json.RawMessage `json:"txn,omitempty"`
}

func main() {
	jsonData := `{
		"type": "txn",
		"msg_id": 1,
		"txn": [
			{"key": 1, "type": "r", "value": null},
			{"key": 1, "type": "w", "value": 6}
		]
	}`
	
	var body TxnListBody
	json.Unmarshal([]byte(jsonData), &body)
	println("Type:", body.Type)
	println("MsgId:", body.MsgId)
	println("Txn length:", len(body.Txn))
	for _, raw := range body.Txn {
		var op TxnOp
		json.Unmarshal(raw, &op)
		println(op.Key, op.Type, fmt.Sprintf("%v", op.Value))
	}
}
