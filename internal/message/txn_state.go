package message

type TxnState int

const (
	TxnStatePreAccepted TxnState = iota
	TxnStateAccepted
	TxnStateApplied
	TxnStateCommitted
)
