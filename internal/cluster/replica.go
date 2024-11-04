package cluster

import (
	"maps"
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
	ts0       message.Timestamp
	ts        message.Timestamp
	highestTs message.Timestamp
	state     message.TxnState
}

type replicaState struct {
	// mapping of: key -> transactions using this key
	keyToTxns map[string]common.Set[message.Transaction]

	txnInfo map[message.Transaction]*txnInfo

	commitsPubSub map[message.Transaction][]chan struct{}
	appliesPubSub map[message.Transaction][]chan struct{}
}

func newReplicaState() *replicaState {
	return &replicaState{
		keyToTxns:     make(map[string]common.Set[message.Transaction]),
		txnInfo:       make(map[message.Transaction]*txnInfo),
		commitsPubSub: make(map[message.Transaction][]chan struct{}),
		appliesPubSub: make(map[message.Transaction][]chan struct{}),
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
	keys common.Set[string],
	ts0 message.Timestamp,
) (message.Timestamp, message.TxnDependencies, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	proposedTs := ts0

	txDeps := r.getDependencies(txn, keys)

	for depTx := range txDeps.Deps {
		info := r.rs.txnInfo[depTx]

		if proposedTs.Less(info.highestTs) {
			proposedTs = message.Timestamp{
				LocalTime:   info.highestTs.LocalTime,
				LogicalTime: info.highestTs.LogicalTime + 1,
				Pid:         r.pid,
			}
		}
	}

	r.rs.txnInfo[txn] = &txnInfo{
		ts0:       ts0,
		ts:        proposedTs,
		highestTs: proposedTs,
		state:     message.TxnStatePreAccepted,
	}

	/*
		No need to filter txDeps on condition ts0_g < proposedTs
		because proposedTs is already greater than any other ts0_g

		This follows from ts0_g <= T_g <= max(T_g | g ~ txn) < proposedTs
	*/

	for key := range keys {
		if _, ok := r.rs.keyToTxns[key]; !ok {
			r.rs.keyToTxns[key] = make(common.Set[message.Transaction])
		}

		r.rs.keyToTxns[key].Add(txn)
	}

	return proposedTs, txDeps, nil
}

func (r *Replica) Accept(
	sender int,
	txn message.Transaction,
	keys common.Set[string],
	ts0 message.Timestamp,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.rs.txnInfo[txn].highestTs.Less(ts) {
		r.rs.txnInfo[txn].highestTs = ts

		/*
			Original article does not contain this statement

			Although it is needed because otherwise consensus
			can deadlock: t_txn can be less than T_txn,
			but when processing Apply(...) we should await
			all dependencies with lower t to be applied
		*/
		r.rs.txnInfo[txn].ts = ts
	}

	r.rs.txnInfo[txn].state = message.TxnStateAccepted

	txnDeps := r.getDependencies(txn, keys)

	for tx := range txnDeps.Deps {
		if !(r.rs.txnInfo[tx].ts0.Less(ts)) {
			txnDeps.Deps.Remove(tx)
		}
	}

	return txnDeps, nil
}

func (r *Replica) Commit(
	sender int,
	txn message.Transaction,
	ts0 message.Timestamp,
	ts message.Timestamp,
	deps message.TxnDependencies,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rs.txnInfo[txn].state = message.TxnStateCommitted

	for _, ch := range r.rs.commitsPubSub[txn] {
		ch <- struct{}{}
	}

	r.rs.commitsPubSub[txn] = nil

	return nil
}

func (r *Replica) Read(
	sender int,
	txn message.Transaction,
	keys common.Set[string],
	ts message.Timestamp,
	deps message.TxnDependencies,
) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	depsCopy := maps.Clone(deps.Deps)
	depsCopy.Add(txn)

	r.awaitCommitted(depsCopy)
	r.awaitApplied(ts, deps.Deps)

	keysSlice := make([]string, 0, len(keys))
	for key := range keys {
		keysSlice = append(keysSlice, key)
	}

	vals, err := r.storage.GetBatch(keysSlice)
	if err != nil {
		// TODO
	}

	reads := make(map[string]string, len(keys))
	for i, k := range keysSlice {
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

	depsCopy := maps.Clone(deps.Deps)
	depsCopy.Add(txn)

	r.awaitCommitted(depsCopy)
	r.awaitApplied(ts, deps.Deps)

	err := r.storage.SetBatch(result)
	if err != nil {
		// TODO
	}

	r.rs.txnInfo[txn].state = message.TxnStateApplied

	for _, ch := range r.rs.appliesPubSub[txn] {
		ch <- struct{}{}
	}

	r.rs.appliesPubSub[txn] = nil

	// TODO finally remove txn

	return nil
}

func (r *Replica) getDependencies(
	txn message.Transaction,
	keys common.Set[string],
) message.TxnDependencies {
	deps := common.Set[message.Transaction]{}

	for key := range keys {
		txs := r.rs.keyToTxns[key]
		for tx := range txs {
			if tx != txn {
				deps.Add(tx)
			}
		}
	}

	return message.TxnDependencies{
		Deps: deps,
	}
}

func (r *Replica) awaitCommitted(txns common.Set[message.Transaction]) {
	var wg sync.WaitGroup

	for tx := range txns {
		if info, ok := r.rs.txnInfo[tx]; !ok {
			continue
		} else if info.state == message.TxnStateCommitted || info.state == message.TxnStateApplied {
			continue
		}

		wg.Add(1)

		waitCh := make(chan struct{})

		r.rs.commitsPubSub[tx] = append(r.rs.commitsPubSub[tx], waitCh)

		go func(tx message.Transaction) {
			<-waitCh

			wg.Done()
		}(tx)
	}

	r.mu.Unlock()

	wg.Wait()

	r.mu.Lock()
}

func (r *Replica) awaitApplied(ts message.Timestamp, txns common.Set[message.Transaction]) {
	var wg sync.WaitGroup

	for tx := range txns {
		if info, ok := r.rs.txnInfo[tx]; !ok {
			continue
		} else if info.state == message.TxnStateApplied {
			continue
		}

		if !(r.rs.txnInfo[tx].ts.Less(ts)) {
			continue
		}

		wg.Add(1)

		waitCh := make(chan struct{})

		r.rs.appliesPubSub[tx] = append(r.rs.appliesPubSub[tx], waitCh)

		go func(tx message.Transaction) {
			<-waitCh

			wg.Done()
		}(tx)
	}

	r.mu.Unlock()

	wg.Wait()

	r.mu.Lock()
}
