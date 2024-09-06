package cluster

import (
	"github.com/eqimd/accord/common"
	"github.com/eqimd/accord/message"
)

type Environment struct {
	Shards           int
	ReplicasPerShard int

	coordinators map[int]*Coordinator
	replicas     map[int]*Replica
}

func NewEnvironment(shardsCount int, replicasPerShard int) *Environment {
	env := &Environment{
		Shards:           shardsCount,
		ReplicasPerShard: replicasPerShard,
	}

	replicas := make(map[int]*Replica, shardsCount*replicasPerShard)
	coordinators := make(map[int]*Coordinator, shardsCount)

	for i := 0; i < shardsCount*replicasPerShard; i++ {
		replicas[i] = NewReplica(i, env)
	}

	for i := 0; i < shardsCount; i++ {
		cpid := shardsCount*replicasPerShard + i
		coordinators[cpid] = NewCoordinator(cpid, env)
	}

	env.replicas = replicas
	env.coordinators = coordinators

	return env
}

func (e *Environment) PreAccept(
	from, to int,
	txn message.Transaction,
	keys common.Set[string],
	ts0 message.Timestamp,
) (message.Timestamp, message.TxnDependencies, error) {
	return e.replicas[to].PreAccept(from, txn, keys, ts0)
}

func (e *Environment) Accept(
	from, to int,
	txn message.Transaction,
	keys common.Set[string],
	ts0 message.Timestamp,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	return e.replicas[to].Accept(from, txn, keys, ts0, ts)
}

func (e *Environment) Commit(
	from, to int,
	txn message.Transaction,
	ts0 message.Timestamp,
	ts message.Timestamp,
	deps message.TxnDependencies,
) error {
	return e.replicas[to].Commit(from, txn, ts0, ts, deps)
}

func (e *Environment) Read(
	from, to int,
	txn message.Transaction,
	keys common.Set[string],
	ts message.Timestamp,
	deps message.TxnDependencies,
) (map[string]string, error) {
	return e.replicas[to].Read(from, txn, keys, ts, deps)
}

func (e *Environment) Apply(
	from, to int,
	txn message.Transaction,
	ts message.Timestamp,
	deps message.TxnDependencies,
	result map[string]string,
) error {
	return e.replicas[to].Apply(from, txn, ts, deps, result)
}
