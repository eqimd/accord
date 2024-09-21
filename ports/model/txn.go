package model

type Txn struct {
	Hash string    `json:"hash"`
	Ts   Timestamp `json:"ts"`
}
