package model

import "github.com/eqimd/accord/internal/message"

type ReadRequest struct {
	Sender      int                     `json:"sender"`
	Txn         message.Transaction     `json:"txn"`
	TsExecution message.Timestamp       `json:"ts_execution"`
	TxnKeys     []string                `json:"txn_keys"`
	Deps        message.TxnDependencies `json:"deps"`
}

type ReadResponse struct {
	Reads map[string]string `json:"reads"`
}
