package model

type ApplyRequest struct {
	Sender      int               `json:"sender"`
	Txn         Txn               `json:"txn"`
	TsExecution Timestamp         `json:"ts_execution"`
	Deps        []Txn             `json:"deps"`
	Result      map[string]string `json:"result"`
}
