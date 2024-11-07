package model

type ReadRequest struct {
	Sender      int       `json:"sender"`
	Txn         Txn       `json:"txn"`
	TsExecution Timestamp `json:"ts_execution"`
	TxnKeys     []string  `json:"txn_keys"`
	Deps        []Txn     `json:"deps"`
}

type ReadResponse struct {
	Reads map[string]string `json:"reads"`
}
