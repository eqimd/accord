package cluster

import (
	"sync"

	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/storage"
	"github.com/eqimd/accord/proto"
	rpc "github.com/eqimd/accord/proto"
)

type State int

const (
	StateRunning State = iota
	StatePaused
	StateStopped
)

type Replica struct {
	state State

	pid     int32
	storage *storage.InMemory

	rs *replicaState
}

type txnInfo struct {
	mu sync.RWMutex

	ts0           *rpc.TxnTimestamp
	ts            *rpc.TxnTimestamp
	highestTs     *rpc.TxnTimestamp
	state         txnState
	keys          []string
	commitsPubSub []chan struct{}
	appliesPubSub []chan struct{}
}

type replicaState struct {
	// mapping of: key -> transactions using this key
	keyToTxnsMu sync.RWMutex
	keyToTxns   map[string]common.Set[txnWrap]

	txnInfo sync.Map
	// txnInfo map[message.Transaction]*txnInfo
}

func (rs *replicaState) getTxnInfo(txn txnWrap) (*txnInfo, bool) {
	var ptr *txnInfo

	val, ok := rs.txnInfo.Load(txn)

	if ok {
		ptr = val.(*txnInfo)
	}

	return ptr, ok
}

func (rs *replicaState) getAndDeleteTxnInfo(txn txnWrap) *txnInfo {
	val, _ := rs.txnInfo.LoadAndDelete(txn)

	return val.(*txnInfo)
}

func (rs *replicaState) setTxnInfo(txn txnWrap, info *txnInfo) {
	rs.txnInfo.Store(txn, info)
}

func newReplicaState() *replicaState {
	return &replicaState{
		keyToTxns: make(map[string]common.Set[txnWrap]),
	}
}

func NewReplica(pid int, storage *storage.InMemory) *Replica {
	return &Replica{
		state:   StateRunning,
		pid:     int32(pid),
		storage: storage,
		rs:      newReplicaState(),
	}
}

func (r *Replica) Pid() int {
	return int(r.pid)
}

func (r *Replica) PreAccept(
	sender int,
	request *rpc.PreAcceptRequest,
) (*rpc.PreAcceptResponse, error) {
	ts0 := request.Ts0
	proposedTs := ts0

	txn := wrapGRPCTxn(request.Txn)

	txDeps := r.getDependencies(txn, request.Keys)

	maxHighest := ts0

	for depTx := range txDeps {
		info, ok := r.rs.getTxnInfo(depTx)
		if !ok {
			continue
		}

		info.mu.RLock()

		if proto.TsLess(maxHighest, info.highestTs) {
			maxHighest = info.highestTs
		}

		info.mu.RUnlock()
	}

	if !proto.TsEqual(maxHighest, ts0) {
		logTime := *maxHighest.LogicalTime + 1
		proposedTs = &proto.TxnTimestamp{
			LocalTime:   maxHighest.LocalTime,
			LogicalTime: &logTime,
			Pid:         &r.pid,
		}
	}

	info := &txnInfo{
		ts0:       ts0,
		ts:        proposedTs,
		highestTs: proposedTs,
		state:     statePreAccepted,
		keys:      request.Keys,
	}

	r.rs.setTxnInfo(txn, info)

	for tx := range txDeps {
		info, ok := r.rs.getTxnInfo(tx)
		if !ok {
			continue
		}

		info.mu.RLock()

		if !proto.TsLess(info.ts0, ts0) {
			delete(txDeps, tx)
		}

		info.mu.RUnlock()
	}

	r.rs.keyToTxnsMu.Lock()
	for _, key := range request.Keys {
		if _, ok := r.rs.keyToTxns[key]; !ok {
			r.rs.keyToTxns[key] = make(common.Set[txnWrap])
		}

		r.rs.keyToTxns[key].Add(txn)
	}
	r.rs.keyToTxnsMu.Unlock()

	deps := make([]*proto.Transaction, 0, len(txDeps))
	for d := range txDeps {
		tx := unwrapTxn(&d)

		deps = append(deps, tx)
	}

	resp := &proto.PreAcceptResponse{
		Ts:   proposedTs,
		Deps: deps,
	}

	return resp, nil
}

func (r *Replica) Accept(
	sender int,
	request *rpc.AcceptRequest,
) (*rpc.AcceptResponse, error) {
	txn := wrapGRPCTxn(request.Txn)
	txnInfo, _ := r.rs.getTxnInfo(txn)

	txnInfo.mu.Lock()

	if proto.TsLess(txnInfo.highestTs, request.Ts) {
		txnInfo.highestTs = request.Ts

		/*
			Original article does not contain this statement

			Although it is needed because otherwise consensus
			can deadlock: t_txn can be less than T_txn,
			but when processing Apply(...) we should await
			all dependencies with lower t to be applied
		*/
		txnInfo.ts = request.Ts
	}

	txnInfo.state = stateAccepted

	txnInfo.mu.Unlock()

	txnDeps := r.getDependencies(txn, request.Keys)

	for tx := range txnDeps {
		info, ok := r.rs.getTxnInfo(tx)
		if !ok {
			continue
		}

		info.mu.RLock()

		if !proto.TsLess(info.ts0, request.Ts) {
			txnDeps.Remove(tx)
		}

		info.mu.RUnlock()
	}

	deps := make([]*proto.Transaction, 0, len(txnDeps))
	for d := range txnDeps {
		tx := unwrapTxn(&d)

		deps = append(deps, tx)
	}

	return &proto.AcceptResponse{
		Deps: deps,
	}, nil
}

func (r *Replica) Commit(
	sender int,
	request *rpc.CommitRequest,
) error {
	txn := wrapGRPCTxn(request.Txn)
	txnInfo, _ := r.rs.getTxnInfo(txn)

	txnInfo.mu.Lock()
	defer txnInfo.mu.Unlock()

	txnInfo.ts = request.Ts
	txnInfo.state = stateCommited

	for _, ch := range txnInfo.commitsPubSub {
		close(ch)
	}

	txnInfo.commitsPubSub = nil

	return nil
}

func (r *Replica) Read(
	sender int,
	request *rpc.ReadRequest,
) (map[string]string, error) {
	r.awaitCommitted(request.Txn, request.Deps)
	r.awaitApplied(request.Ts, request.Deps)

	vals, err := r.storage.GetBatch(request.Keys)
	if err != nil {
		// TODO
	}

	reads := make(map[string]string, len(request.Keys))
	for i, k := range request.Keys {
		reads[k] = vals[i]
	}

	return reads, nil
}

func (r *Replica) Apply(
	sender int,
	request *rpc.ApplyRequest,
) error {
	r.awaitCommitted(request.Txn, request.Deps)
	r.awaitApplied(request.Ts, request.Deps)

	err := r.storage.SetBatch(request.Result)
	if err != nil {
		// TODO
	}

	wrap := wrapGRPCTxn(request.Txn)

	txnInfo := r.rs.getAndDeleteTxnInfo(wrap)

	txnInfo.mu.Lock()

	txnInfo.state = stateApplied
	for _, ch := range txnInfo.appliesPubSub {
		close(ch)
	}

	txnInfo.appliesPubSub = nil

	txnInfo.mu.Unlock()

	keys := txnInfo.keys

	r.rs.keyToTxnsMu.Lock()
	for _, k := range keys {
		delete(r.rs.keyToTxns[k], wrap)
	}
	r.rs.keyToTxnsMu.Unlock()

	return nil
}

func (r *Replica) getDependencies(
	txn txnWrap,
	keys []string,
) common.Set[txnWrap] {
	deps := common.Set[txnWrap]{}

	r.rs.keyToTxnsMu.RLock()
	for _, key := range keys {
		txs := r.rs.keyToTxns[key]

		deps.Union(txs)
	}
	r.rs.keyToTxnsMu.RUnlock()

	delete(deps, txn)

	return deps
}

func (r *Replica) awaitCommitted(origTxn *rpc.Transaction, txns []*rpc.Transaction) {
	chans := make([]chan struct{}, 0, len(txns)+1)
	for range len(txns) + 1 {
		chans = append(chans, make(chan struct{}))
	}

	processTxnFunc := func(tx *rpc.Transaction, waitCh chan struct{}) {
		wrap := wrapGRPCTxn(tx)

		info, ok := r.rs.getTxnInfo(wrap)
		if !ok {
			close(waitCh)

			return
		}

		info.mu.Lock()
		defer info.mu.Unlock()

		if info.state == stateCommited || info.state == stateApplied {
			close(waitCh)

			return
		}

		info.commitsPubSub = append(info.commitsPubSub, waitCh)
	}

	processTxnFunc(origTxn, chans[len(txns)])

	for i, tx := range txns {
		processTxnFunc(tx, chans[i])
	}

	<-chans[len(txns)]
	for _, ch := range chans {
		<-ch
	}
}

func (r *Replica) awaitApplied(ts *rpc.TxnTimestamp, txns []*rpc.Transaction) {
	chans := make([]chan struct{}, 0, len(txns))

	for _, tx := range txns {
		wrap := wrapGRPCTxn(tx)

		info, ok := r.rs.getTxnInfo(wrap)
		if !ok {
			continue
		}

		info.mu.Lock()

		if info.state == stateApplied {
			info.mu.Unlock()

			continue
		}

		if !proto.TsLess(info.ts, ts) {
			info.mu.Unlock()

			continue
		}

		waitCh := make(chan struct{})

		info.appliesPubSub = append(info.appliesPubSub, waitCh)

		chans = append(chans, waitCh)

		info.mu.Unlock()
	}

	for _, ch := range chans {
		<-ch
	}
}

func (r *Replica) Snapshot() (map[string]string, error) {
	return r.storage.Snapshot()
}
