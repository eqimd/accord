package model

type ReadRequest struct {
	TxnHash     string    `json:"txn_hash"`
	TsExecution Timestamp `json:"ts_execution"`
	TxnKeys     []string  `json:"txn_keys"`
	Deps        []string  `json:"deps"`
}

type ReadResponse struct {
}
