package model

type CommitRequest struct {
	TxnHash     string    `json:"txn_hash"`
	TsProposed  Timestamp `json:"ts_proposed"`
	TsExecution Timestamp `json:"ts_execution"`
	Deps        []string  `json:"deps"`
}

type CommitResponse struct {
}
