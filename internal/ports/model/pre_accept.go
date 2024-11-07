package model

type PreAcceptRequest struct {
	Sender     int       `json:"sender"`
	Txn        Txn       `json:"txn"`
	TsProposed Timestamp `json:"ts_proposed"`
	TxnKeys    []string  `json:"txn_keys"`
}

type PreAcceptResponse struct {
	TsProposed Timestamp `json:"ts_proposed"`
	Deps       []Txn     `json:"deps"`
}
