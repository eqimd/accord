package cluster

import (
	"sync"

	"github.com/eqimd/accord/common"
	"github.com/eqimd/accord/message"
)

type State int

const (
	StateRunning State = iota
	StatePaused
	StateStopped
)

type TxnState int

const (
	PreAccepted TxnState = iota
	Accepted
	Applied
	Committed
)

type Replica struct {
	mu sync.Mutex

	state State

	pid     int
	env     *Environment
	storage *Storage

	rs *replicaState
}

type txnInfo struct {
	ts0       message.Timestamp
	ts        message.Timestamp
	highestTs message.Timestamp
	state     TxnState
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

func NewReplica(pid int, env *Environment) *Replica {
	return &Replica{
		state:   StateRunning,
		pid:     pid,
		env:     env,
		storage: NewStorage(),
		rs:      newReplicaState(),
	}
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
		state:     PreAccepted,
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
		r.rs.txnInfo[txn].ts = ts
	}

	r.rs.txnInfo[txn].state = Accepted

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

	r.rs.txnInfo[txn].state = Committed

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

	r.awaitCommitted(deps.Deps)
	r.awaitApplied(ts, deps.Deps)

	// r.awaitCommittedAndApplied(ts, deps.Deps)

	reads := map[string]string{}
	for key := range keys {
		val := r.storage.Get(key)

		reads[key] = val
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

	r.awaitCommitted(deps.Deps)
	r.awaitApplied(ts, deps.Deps)

	// r.awaitCommittedAndApplied(ts, deps.Deps)

	for k, v := range result {
		r.storage.Set(k, v)
	}

	r.rs.txnInfo[txn].state = Applied

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
		} else if info.state == Committed || info.state == Applied {
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
		} else if info.state == Applied {
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

func (r *Replica) awaitCommittedAndApplied(ts message.Timestamp, txns common.Set[message.Transaction]) {
	var wg sync.WaitGroup

	for tx := range txns {
		info, ok := r.rs.txnInfo[tx]
		if !ok {
			continue
		}

		if info.state == Applied {
			continue
		}

		if info.state != Committed {
			waitChCommitted := make(chan struct{})

			r.rs.commitsPubSub[tx] = append(r.rs.commitsPubSub[tx], waitChCommitted)

			wg.Add(1)
			go func() {
				<-waitChCommitted

				wg.Done()
			}()
		}

		if !(r.rs.txnInfo[tx].ts.Less(ts)) {
			continue
		}

		waitChApplied := make(chan struct{})

		r.rs.appliesPubSub[tx] = append(r.rs.appliesPubSub[tx], waitChApplied)

		wg.Add(1)
		go func(tsTxn, tsCmp message.Timestamp) {
			<-waitChApplied

			wg.Done()
		}(ts, r.rs.txnInfo[tx].ts)
	}

	r.mu.Unlock()

	wg.Wait()

	r.mu.Lock()
}
