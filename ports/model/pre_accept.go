package model

type PreAcceptRequest struct {
	TxnHash    string    `json:"txn_hash"`
	TsProposed Timestamp `json:"ts_proposed"`
	TxnKeys    []string  `json:"txn_keys"`
}

type PreAcceptResponse struct {
	TsProposed Timestamp `json:"ts_proposed"`
	Deps []string `json:"deps"`
}
