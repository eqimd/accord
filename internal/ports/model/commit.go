package model

type CommitRequest struct {
	Sender      int       `json:"sender"`
	TxnHash     string    `json:"txn_hash"`
	TsProposed  Timestamp `json:"ts_proposed"`
	TsExecution Timestamp `json:"ts_execution"`
	Deps        []string  `json:"deps"`
}

type CommitResponse struct{}
