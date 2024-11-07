package model

type CommitRequest struct {
	Sender      int       `json:"sender"`
	Txn         Txn       `json:"txn"`
	TsProposed  Timestamp `json:"ts_proposed"`
	TsExecution Timestamp `json:"ts_execution"`
	Deps        []Txn     `json:"deps"`
}
