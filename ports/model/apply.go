package model

type ApplyRequest struct {
	Sender      int               `json:"sender"`
	TxnHash     string            `json:"txn_hash"`
	TsExecution Timestamp         `json:"ts_execution"`
	Deps        []Txn             `json:"deps"`
	Result      map[string]string `json:"result"`
}

type ApplyResponse struct{}
