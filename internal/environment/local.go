package environment

import (
	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
)

type Local struct {
	shardToReplicas    map[int]map[int]*cluster.Replica
	replicas           map[int]*cluster.Replica
	replicaPidsByShard map[int]common.Set[int]
}

func NewLocal(shardToReplicas map[int]map[int]*cluster.Replica) *Local {
	env := &Local{
		shardToReplicas: shardToReplicas,
	}

	replicas := make(map[int]*cluster.Replica)
	replicaPidsByShard := map[int]common.Set[int]{}

	for shardID, reps := range shardToReplicas {
		replicaPidsByShard[shardID] = common.Set[int]{}

		for rPid, r := range reps {
			replicas[rPid] = r
			replicaPidsByShard[shardID].Add(rPid)
		}
	}

	env.replicas = replicas
	env.replicaPidsByShard = replicaPidsByShard

	return env
}

func (e *Local) PreAccept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts0 message.Timestamp,
) (message.Timestamp, message.TxnDependencies, error) {
	return e.replicas[to].PreAccept(from, txn, keys, ts0)
}

func (e *Local) Accept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts0 message.Timestamp,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	return e.replicas[to].Accept(from, txn, keys, ts0, ts)
}

func (e *Local) Commit(
	from, to int,
	txn message.Transaction,
	ts0 message.Timestamp,
	ts message.Timestamp,
	deps message.TxnDependencies,
) error {
	return e.replicas[to].Commit(from, txn, ts0, ts, deps)
}

func (e *Local) Read(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
	deps message.TxnDependencies,
) (map[string]string, error) {
	return e.replicas[to].Read(from, txn, keys, ts, deps)
}

func (e *Local) Apply(
	from, to int,
	txn message.Transaction,
	ts message.Timestamp,
	deps message.TxnDependencies,
	result map[string]string,
) error {
	return e.replicas[to].Apply(from, txn, ts, deps, result)
}

func (e *Local) ReplicaPidsByShard(shardID int) common.Set[int] {
	return e.replicaPidsByShard[shardID]
}
