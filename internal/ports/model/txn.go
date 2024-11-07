package model

import (
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
)

type Txn struct {
	Hash string    `json:"hash"`
	Ts   Timestamp `json:"ts"`
}

func (t *Txn) ToMessageTxn() message.Transaction {
	return message.Transaction{
		TxnHash:   t.Hash,
		Timestamp: t.Ts.ToMessageTimestamp(),
	}
}

func ModelDepsFromMessage(deps message.TxnDependencies) []Txn {
	modelDeps := make([]Txn, 0, len(deps.Deps))
	for d := range deps.Deps {
		md := Txn{
			Hash: d.TxnHash,
			Ts:   FromMessageTimestamp(d.Timestamp),
		}
		modelDeps = append(modelDeps, md)
	}

	return modelDeps
}

func MessageDepsFromModel(deps []Txn) message.TxnDependencies {
	msgDeps := common.Set[message.Transaction]{}

	for _, d := range deps {
		msgDeps.Add(d.ToMessageTxn())
	}

	return message.TxnDependencies{
		Deps: msgDeps,
	}
}
