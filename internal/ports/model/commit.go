package model

import "github.com/eqimd/accord/internal/message"

type CommitRequest struct {
	Sender int                 `json:"sender"`
	Txn    message.Transaction `json:"txn"`
	Ts     message.Timestamp   `json:"ts"`
}
