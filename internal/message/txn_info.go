package message

type TxnInfo struct {
	Txn         Transaction
	State       TxnState
	Deps        TxnDependencies
	TsProposed  Timestamp //ts0
	TsExecution Timestamp // ts
}
