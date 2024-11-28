package provider

import (
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
)

type Environment interface {
	PreAccept(
		from, to int,
		txn message.Transaction,
		keys []string,
		ts0 message.Timestamp,
	) (message.Timestamp, message.TxnDependencies, error)

	Accept(
		from, to int,
		txn message.Transaction,
		keys []string,
		ts message.Timestamp,
	) (message.TxnDependencies, error)

	Commit(
		from, to int,
		txn message.Transaction,
	) error

	Read(
		from, to int,
		txn message.Transaction,
		keys []string,
		ts message.Timestamp,
		deps message.TxnDependencies,
	) (map[string]string, error)

	Apply(
		from, to int,
		txn message.Transaction,
		ts message.Timestamp,
		deps message.TxnDependencies,
		result map[string]string,
	) error

	ReplicaPidsByShard(shardID int) common.Set[int]
}
