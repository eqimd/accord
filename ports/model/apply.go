package model

type ApplyRequest struct {
	TxnHash     string            `json:"txn_hash"`
	TsExecution Timestamp         `json:"ts_execution"`
	Deps        []string          `json:"deps"`
	Result      map[string]string `json:"result"`
}

type ApplyResponse struct {
}
