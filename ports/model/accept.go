package model

type AcceptRequest struct {
	TxnHash     string    `json:"txn_hash"`
	TsProposed  Timestamp `json:"ts_proposed"`
	TxnKeys     []string  `json:"txn_keys"`
	TsExecution Timestamp `json:"ts_execution"`
}

type AcceptResponse struct {
}
