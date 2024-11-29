package message

type Transaction struct {
	TxnHash   string `json:"txn_hash"`
	Timestamp Timestamp `json:"timestamp"`
}
