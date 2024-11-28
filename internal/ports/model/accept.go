package model

type AcceptRequest struct {
	Sender      int       `json:"sender"`
	Txn         Txn       `json:"txn"`
	TxnKeys     []string  `json:"txn_keys"`
	TsExecution Timestamp `json:"ts_execution"`
}

type AcceptResponse struct {
	Deps []Txn `json:"deps"`
}
