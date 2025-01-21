package coordinator

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eqimd/accord/internal/environment"
	"github.com/eqimd/accord/internal/query"
	"github.com/eqimd/accord/internal/sharding"
	"github.com/eqimd/accord/proto"
)

type Coordinator struct {
	pid           int
	env           *environment.GRPCEnv
	sharding      *sharding.Hash
	queryExecutor *query.Executor

	slowPaths *atomic.Uint64
}

func NewCoordinator(
	pid int,
	env *environment.GRPCEnv,
	sharding *sharding.Hash,
	queryExecutor *query.Executor,
) *Coordinator {
	return &Coordinator{
		pid:           pid,
		env:           env,
		sharding:      sharding,
		queryExecutor: queryExecutor,
		slowPaths:     &atomic.Uint64{},
	}
}

func (c *Coordinator) genNewTs() *proto.TxnTimestamp {
	local := uint64(time.Now().UnixNano())
	logical := int32(0)
	pid := int32(c.pid)

	ts := &proto.TxnTimestamp{
		LocalTime:   &local,
		LogicalTime: &logical,
		Pid:         &pid,
	}

	return ts
}

func (c *Coordinator) proposeTransaction(
	txn *proto.Transaction,
	ts0 *proto.TxnTimestamp,
	shardToKeys map[int][]string,
) (tsCommit *proto.TxnTimestamp, shardToDeps map[int][]*proto.Transaction) {
	pid := int32(c.pid)

	var wg sync.WaitGroup
	var mu sync.Mutex

	proposedMax := ts0
	ts0PerShardQuorums := map[int]struct{}{}

	shardToDeps = map[int][]*proto.Transaction{}

	for shardID, keys := range shardToKeys {
		replicaPids := c.env.ReplicaPidsByShard(shardID)
		wg.Add(1)

		shardChan := make(chan *proto.PreAcceptResponse, len(replicaPids))

		for replicaPid := range replicaPids {
			go func(shardID, rpid, replicasCount int) {
				req := &proto.PreAcceptRequest{
					Txn:    txn,
					Ts0:    ts0,
					Keys:   keys,
					Sender: &pid,
				}

				resp, err := c.env.PreAccept(c.pid, rpid, req)
				if err != nil {
					slog.Error("preaccept error", slog.Any("error", err))
					// TODO
				}

				shardChan <- resp
			}(shardID, replicaPid, len(replicaPids))
		}

		go func(replicasCount int) {
			respCount := 0
			ts0ShardQuorumCnt := 0

			for resp := range shardChan {
				respCount++

				mu.Lock()

				if proto.TsEqual(resp.Ts, ts0) {
					ts0ShardQuorumCnt++

					if 2*ts0ShardQuorumCnt > replicasCount {
						ts0PerShardQuorums[shardID] = struct{}{}
					}
				}

				if proto.TsLess(proposedMax, resp.Ts) {
					proposedMax = resp.Ts
				}

				shardToDeps[shardID] = append(shardToDeps[shardID], resp.Deps...)

				mu.Unlock()

				if respCount == replicasCount {
					break
				}
			}

			wg.Done()
		}(len(replicaPids))
	}

	wg.Wait()

	tsCommit = ts0

	// Check for fast-path quorums
	if len(ts0PerShardQuorums) != len(shardToKeys) {
		c.slowPaths.Add(1)

		slog.Info("Slow path count", "count", c.slowPaths.Load())
		// No fast-path quorum; perform second round-trip

		tsCommit = proposedMax
		shardToDeps = map[int][]*proto.Transaction{}

		wg := sync.WaitGroup{}
		mu := sync.Mutex{}

		for shardID, keys := range shardToKeys {
			replicaPids := c.env.ReplicaPidsByShard(shardID)
			wg.Add(1)

			shardChan := make(chan *proto.AcceptResponse, len(replicaPids))

			for replicaPid := range replicaPids {
				go func(shardID, rpid int) {
					req := &proto.AcceptRequest{
						Txn:    txn,
						Keys:   keys,
						Ts:     tsCommit,
						Sender: &pid,
					}

					resp, err := c.env.Accept(c.pid, rpid, req)
					if err != nil {
						slog.Error("accept error", slog.Any("error", err))
						// TODO
					}

					shardChan <- resp
				}(shardID, replicaPid)
			}

			go func(replicasCount int) {
				respCount := 0

				for resp := range shardChan {
					respCount++

					mu.Lock()

					shardToDeps[shardID] = append(shardToDeps[shardID], resp.Deps...)

					mu.Unlock()

					if respCount == replicasCount {
						break
					}
				}

				wg.Done()
			}(len(replicaPids))
		}

		wg.Wait()
	}

	for shardID := range shardToKeys {
		replicaPids := c.env.ReplicaPidsByShard(shardID)

		for replicaPid := range replicaPids {
			go func(rpid int) {
				req := &proto.CommitRequest{
					Txn:    txn,
					Ts:     tsCommit,
					Sender: &pid,
				}
				err := c.env.Commit(c.pid, rpid, req)
				if err != nil {
					slog.Error("commit error", slog.Any("error", err))
					// TODO
				}
			}(replicaPid)
		}
	}

	return
}

func (c *Coordinator) apply(
	txn *proto.Transaction,
	tsCommit *proto.TxnTimestamp,
	result map[string]string,
	shardToKeys map[int][]string,
	shardToDeps map[int][]*proto.Transaction,
) {
	pid := int32(c.pid)

	for shardID := range shardToKeys {
		replicaPids := c.env.ReplicaPidsByShard(shardID)

		for replicaPid := range replicaPids {
			go func(shardID, rpid int) {
				d := shardToDeps[shardID]

				req := &proto.ApplyRequest{
					Txn:    txn,
					Ts:     tsCommit,
					Deps:   d,
					Result: result,
					Sender: &pid,
				}

				err := c.env.Apply(c.pid, rpid, req)
				if err != nil {
					slog.Error("apply error", slog.Any("error", err))
					// TODO
				}
			}(shardID, replicaPid)
		}
	}
}

func (c *Coordinator) read(
	txn *proto.Transaction,
	tsCommit *proto.TxnTimestamp,
	shardToKeys map[int][]string,
	shardToDeps map[int][]*proto.Transaction,
) map[string]string {
	pid := int32(c.pid)

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	allReads := map[string]string{}

	for shardID, keys := range shardToKeys {
		wg.Add(1)

		var replicaPid int
		pidsByShard := c.env.ReplicaPidsByShard(shardID)
		for k := range pidsByShard {
			replicaPid = k
			break
		}

		if _, ok := pidsByShard[c.pid]; ok {
			replicaPid = c.pid
		}

		shardDeps := shardToDeps[shardID]

		keys := keys

		go func() {
			defer wg.Done()

			req := &proto.ReadRequest{
				Txn:    txn,
				Keys:   keys,
				Ts:     tsCommit,
				Deps:   shardDeps,
				Sender: &pid,
			}

			reads, err := c.env.Read(
				c.pid,
				replicaPid,
				req,
			)
			if err != nil {
				slog.Error("read error", slog.Any("error", err))
				// TODO
			}
			mu.Lock()

			for k, v := range reads {
				allReads[k] = v
			}

			mu.Unlock()
		}()
	}

	wg.Wait()

	return allReads
}

func (c *Coordinator) Exec(query string) (string, error) {
	ts0 := c.genNewTs()

	keys, err := c.queryExecutor.QueryKeys(query)
	if err != nil {
		slog.Error("keys error")
		// TODO
	}

	shardToKeys := c.sharding.ShardToKeys(keys)

	txn := &proto.Transaction{
		Hash:      &query,
		Timestamp: ts0,
	}

	tsCommit, shardToDeps := c.proposeTransaction(txn, ts0, shardToKeys)

	allReads := c.read(
		txn,
		tsCommit,
		shardToKeys,
		shardToDeps,
	)

	result, writes, err := c.queryExecutor.Execute(query, allReads)
	if err != nil {
		return "", fmt.Errorf("cannot execute query: %w", err)
	}

	c.apply(
		txn,
		tsCommit,
		writes,
		shardToKeys,
		shardToDeps,
	)

	return result, nil
}

func (c *Coordinator) Put(vals map[string]string) error {
	ts0 := c.genNewTs()

	var b strings.Builder
	keys := make([]string, 0, len(vals))

	for k, v := range vals {
		keys = append(keys, k)

		_, _ = b.WriteString(k)
		_, _ = b.WriteString(v)
	}

	query := b.String()

	shardToKeys := c.sharding.ShardToKeys(keys)

	txn := &proto.Transaction{
		Hash:      &query,
		Timestamp: ts0,
	}

	tsCommit, shardToDeps := c.proposeTransaction(txn, ts0, shardToKeys)

	c.apply(
		txn,
		tsCommit,
		vals,
		shardToKeys,
		shardToDeps,
	)

	return nil
}

func (c *Coordinator) Get(keys []string) (map[string]string, error) {
	ts0 := c.genNewTs()

	query := strings.Join(keys, ";")

	shardToKeys := c.sharding.ShardToKeys(keys)

	txn := &proto.Transaction{
		Hash:      &query,
		Timestamp: ts0,
	}

	tsCommit, shardToDeps := c.proposeTransaction(txn, ts0, shardToKeys)

	allReads := c.read(
		txn,
		tsCommit,
		shardToKeys,
		shardToDeps,
	)

	c.apply(
		txn,
		tsCommit,
		map[string]string{},
		shardToKeys,
		shardToDeps,
	)

	return allReads, nil
}

func (c *Coordinator) Snapshot() (*environment.SnapshotAll, error) {
	return c.env.SnapshotAll(c.pid)
}
