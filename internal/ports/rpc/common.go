package rpc

import "github.com/eqimd/accord/internal/message"

func TxnToGrpc(txn *message.Transaction) *Transaction {
	return &Transaction{
		Hash:      &txn.TxnHash,
		Timestamp: TsToGrpc(&txn.Timestamp),
	}
}

func TxnFromGrpc(txn *Transaction) message.Transaction {
	return message.Transaction{
		TxnHash:   *txn.Hash,
		Timestamp: TsFromGrpc(txn.Timestamp),
	}
}

func TsToGrpc(ts *message.Timestamp) *TxnTimestamp {
	logicalTime := int32(ts.LogicalTime)
	pid := int32(ts.Pid)

	return &TxnTimestamp{
		LocalTime:   &ts.LocalTime,
		LogicalTime: &logicalTime,
		Pid:         &pid,
	}
}

func TsFromGrpc(ts *TxnTimestamp) message.Timestamp {
	return message.Timestamp{
		LocalTime:   *ts.LocalTime,
		LogicalTime: int(*ts.LogicalTime),
		Pid:         int(*ts.Pid),
	}
}

func DepsToGrpc(deps *message.TxnDependencies) []*Transaction {
	txDeps := make([]*Transaction, 0, len(deps.Deps))

	for _, d := range deps.Deps {
		txDeps = append(txDeps, TxnToGrpc(&d))
	}

	return txDeps
}

func DepsFromGrpc(deps []*Transaction) message.TxnDependencies {
	txnDeps := make([]message.Transaction, 0, len(deps))

	for _, d := range deps {
		txnDeps = append(txnDeps, TxnFromGrpc(d))
	}

	return message.TxnDependencies{
		Deps: txnDeps,
	}
}
