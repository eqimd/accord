package model

import "github.com/eqimd/accord/internal/message"

type ApplyRequest struct {
	Sender      int                     `json:"sender"`
	Txn         Txn                     `json:"txn"`
	TsExecution message.Timestamp       `json:"ts_execution"`
	Deps        message.TxnDependencies `json:"deps"`
	Result      map[string]string       `json:"result"`
}
