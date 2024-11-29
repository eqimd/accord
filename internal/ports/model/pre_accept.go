package model

import "github.com/eqimd/accord/internal/message"

type PreAcceptRequest struct {
	Sender     int                 `json:"sender"`
	Txn        message.Transaction `json:"txn"`
	TsProposed message.Timestamp   `json:"ts_proposed"`
	TxnKeys    []string            `json:"txn_keys"`
}

type PreAcceptResponse struct {
	TsProposed message.Timestamp       `json:"ts_proposed"`
	Deps       message.TxnDependencies `json:"deps"`
}
