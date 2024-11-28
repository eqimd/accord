package model

type CommitRequest struct {
	Sender      int       `json:"sender"`
	Txn         Txn       `json:"txn"`
}
