package model

type ReadRequest struct {
	Sender      int       `json:"sender"`
	TxnHash     string    `json:"txn_hash"`
	TsExecution Timestamp `json:"ts_execution"`
	TxnKeys     []string  `json:"txn_keys"`
	Deps        []Txn     `json:"deps"`
}

type ReadResponse struct {
	Reads map[string]string `json:"reads"`
}
