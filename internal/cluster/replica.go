package cluster

import (
	"sync"

	"github.com/eqimd/accord/internal/cluster/provider"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
)

type State int

const (
	StateRunning State = iota
	StatePaused
	StateStopped
)

type Replica struct {
	mu sync.Mutex

	state State

	pid     int
	storage provider.Storage

	rs *replicaState
}

type txnInfo struct {
	ts0           message.Timestamp
	ts            message.Timestamp
	highestTs     message.Timestamp
	state         message.TxnState
	keys          []string
	commitsPubSub []chan struct{}
	appliesPubSub []chan struct{}
}

type replicaState struct {
	// mapping of: key -> transactions using this key
	keyToTxns map[string]common.Set[message.Transaction]

	txnInfo map[message.Transaction]*txnInfo
}

func newReplicaState() *replicaState {
	return &replicaState{
		keyToTxns: make(map[string]common.Set[message.Transaction]),
		txnInfo:   make(map[message.Transaction]*txnInfo),
	}
}

func NewReplica(pid int, storage provider.Storage) *Replica {
	return &Replica{
		state:   StateRunning,
		pid:     pid,
		storage: storage,
		rs:      newReplicaState(),
	}
}

func (r *Replica) Pid() int {
	return r.pid
}

func (r *Replica) PreAccept(
	sender int,
	txn message.Transaction,
	keys []string,
	ts0 message.Timestamp,
) (message.Timestamp, message.TxnDependencies, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	proposedTs := ts0

	txDeps := r.getDependencies(txn, keys)

	maxHighest := ts0

	for depTx := range txDeps {
		info := r.rs.txnInfo[depTx]

		if maxHighest.Less(info.highestTs) {
			maxHighest = info.highestTs
		}
	}

	if maxHighest != ts0 {
		proposedTs = message.Timestamp{
			LocalTime:   maxHighest.LocalTime,
			LogicalTime: maxHighest.LogicalTime + 1,
			Pid:         r.pid,
		}
	}

	r.rs.txnInfo[txn] = &txnInfo{
		ts0:       ts0,
		ts:        proposedTs,
		highestTs: proposedTs,
		state:     message.TxnStatePreAccepted,
		keys:      keys,
	}

	for tx := range txDeps {
		if !(r.rs.txnInfo[tx].ts0.Less(ts0)) {
			delete(txDeps, tx)
		}
	}

	for _, key := range keys {
		if _, ok := r.rs.keyToTxns[key]; !ok {
			r.rs.keyToTxns[key] = make(common.Set[message.Transaction])
		}

		r.rs.keyToTxns[key].Add(txn)
	}

	return proposedTs, message.TxnDependencies{Deps: txDeps.Slice()}, nil
}

func (r *Replica) Accept(
	sender int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	txnInfo := r.rs.txnInfo[txn]

	if txnInfo.highestTs.Less(ts) {
		txnInfo.highestTs = ts

		/*
			Original article does not contain this statement

			Although it is needed because otherwise consensus
			can deadlock: t_txn can be less than T_txn,
			but when processing Apply(...) we should await
			all dependencies with lower t to be applied
		*/
		txnInfo.ts = ts
	}

	txnInfo.state = message.TxnStateAccepted

	txnDeps := r.getDependencies(txn, keys)

	for tx := range txnDeps {
		if !(r.rs.txnInfo[tx].ts0.Less(ts)) {
			txnDeps.Remove(tx)
		}
	}

	return message.TxnDependencies{Deps: txnDeps.Slice()}, nil
}

func (r *Replica) Commit(
	sender int,
	txn message.Transaction,
	ts message.Timestamp,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	txnInfo := r.rs.txnInfo[txn]

	txnInfo.ts = ts
	txnInfo.state = message.TxnStateCommitted

	for _, ch := range txnInfo.commitsPubSub {
		ch <- struct{}{}
	}

	txnInfo.commitsPubSub = nil

	return nil
}

func (r *Replica) Read(
	sender int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
	deps message.TxnDependencies,
) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.awaitCommitted(txn, deps.Deps)
	r.awaitApplied(ts, deps.Deps)

	vals, err := r.storage.GetBatch(keys)
	if err != nil {
		// TODO
	}

	reads := make(map[string]string, len(keys))
	for i, k := range keys {
		reads[k] = vals[i]
	}

	return reads, nil
}

func (r *Replica) Apply(
	sender int,
	txn message.Transaction,
	ts message.Timestamp,
	deps message.TxnDependencies,
	result map[string]string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.awaitCommitted(txn, deps.Deps)
	r.awaitApplied(ts, deps.Deps)

	err := r.storage.SetBatch(result)
	if err != nil {
		// TODO
	}

	txnInfo := r.rs.txnInfo[txn]

	txnInfo.state = message.TxnStateApplied

	for _, ch := range txnInfo.appliesPubSub {
		ch <- struct{}{}
	}

	txnInfo.appliesPubSub = nil

	// for _, k := range txnInfo.keys {
	// 	delete(r.rs.keyToTxns[k], txn)
	// }

	// delete(r.rs.txnInfo, txn)

	return nil
}

func (r *Replica) getDependencies(
	txn message.Transaction,
	keys []string,
) common.Set[message.Transaction] {
	deps := common.Set[message.Transaction]{}

	for _, key := range keys {
		txs := r.rs.keyToTxns[key]

		deps.Union(txs)
	}

	delete(deps, txn)

	return deps
}

func (r *Replica) awaitCommitted(origTxn message.Transaction, txns []message.Transaction) {
	var wg sync.WaitGroup

	processTxnFunc := func(tx message.Transaction) {
		info, ok := r.rs.txnInfo[tx]
		if !ok {
			return
		}

		if info.state == message.TxnStateCommitted || info.state == message.TxnStateApplied {
			return
		}

		wg.Add(1)

		waitCh := make(chan struct{})

		info.commitsPubSub = append(info.commitsPubSub, waitCh)

		go func(tx message.Transaction) {
			<-waitCh

			wg.Done()
		}(tx)
	}

	processTxnFunc(origTxn)

	for _, tx := range txns {
		processTxnFunc(tx)
	}

	r.mu.Unlock()

	wg.Wait()

	r.mu.Lock()
}

func (r *Replica) awaitApplied(ts message.Timestamp, txns []message.Transaction) {
	var wg sync.WaitGroup

	for _, tx := range txns {
		info, ok := r.rs.txnInfo[tx]
		if !ok {
			continue
		}

		if info.state == message.TxnStateApplied {
			continue
		}

		if !(info.ts.Less(ts)) {
			continue
		}

		wg.Add(1)

		waitCh := make(chan struct{})

		info.appliesPubSub = append(info.appliesPubSub, waitCh)

		go func(tx message.Transaction) {
			<-waitCh

			wg.Done()
		}(tx)
	}

	r.mu.Unlock()

	wg.Wait()

	r.mu.Lock()
}

func (r *Replica) Snapshot() (map[string]string, error) {
	return r.storage.Snapshot()
}
