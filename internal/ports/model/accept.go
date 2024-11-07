package model

type AcceptRequest struct {
	Sender      int       `json:"sender"`
	Txn         Txn       `json:"txn"`
	TsProposed  Timestamp `json:"ts_proposed"`
	TxnKeys     []string  `json:"txn_keys"`
	TsExecution Timestamp `json:"ts_execution"`
}

type AcceptResponse struct {
	Deps []Txn `json:"deps"`
}
