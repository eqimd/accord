package replica

import "github.com/eqimd/accord/proto"

type txnState string

const (
	statePreAccepted txnState = "preAccepted"
	stateAccepted    txnState = "accepted"
	stateCommited    txnState = "commited"
	stateApplied     txnState = "applied"
)

type txnWrap struct {
	hash    string
	local   uint64
	logical int32
	pid     int32
}

func wrapGRPCTxn(txn *proto.Transaction) txnWrap {
	return txnWrap{
		hash:    *txn.Hash,
		local:   *txn.Timestamp.LocalTime,
		logical: *txn.Timestamp.LogicalTime,
		pid:     *txn.Timestamp.Pid,
	}
}

func unwrapTxn(w *txnWrap) *proto.Transaction {
	return &proto.Transaction{
		Hash: &w.hash,
		Timestamp: &proto.TxnTimestamp{
			LocalTime:   &w.local,
			LogicalTime: &w.logical,
			Pid:         &w.pid,
		},
	}
}
