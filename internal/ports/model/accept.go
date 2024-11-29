package model

import "github.com/eqimd/accord/internal/message"

type AcceptRequest struct {
	Sender      int                 `json:"sender"`
	Txn         message.Transaction `json:"txn"`
	TxnKeys     []string            `json:"txn_keys"`
	TsExecution message.Timestamp   `json:"ts_execution"`
}

type AcceptResponse struct {
	Deps message.TxnDependencies `json:"deps"`
}
