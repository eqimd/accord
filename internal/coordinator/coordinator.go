package coordinator

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eqimd/accord/internal/environment"
	"github.com/eqimd/accord/internal/query"
	"github.com/eqimd/accord/internal/sharding"
	"github.com/eqimd/accord/proto"
)

/*
type monoClock struct {
	val atomic.Uint64
}

func (c *monoClock) getTime() uint64 {
	v := c.val.Add(1)

	return v
}
*/

type Coordinator struct {
	pid           int
	env           *environment.GRPCEnv
	sharding      *sharding.Hash
	queryExecutor *query.Executor
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
	}
}

func (c *Coordinator) Exec(query string) (string, error) {
	local := uint64(time.Now().UnixNano())
	logical := int32(0)
	pid := int32(c.pid)

	ts0 := &proto.TxnTimestamp{
		LocalTime:   &local,
		LogicalTime: &logical,
		Pid:         &pid,
	}

	keys, err := c.queryExecutor.QueryKeys(query)
	if err != nil {
		slog.Error("keys error")
		// TODO
	}

	shardToKeys := c.sharding.ShardToKeys(keys)

	shardToDeps := map[int][]*proto.Transaction{}

	proposedMax := ts0
	ts0PerShardQuorums := map[int]struct{}{}

	var wg sync.WaitGroup
	var mu sync.Mutex

	txn := &proto.Transaction{
		Hash:      &query,
		Timestamp: ts0,
	}

	for shardID, keys := range shardToKeys {
		replicaPids := c.env.ReplicaPidsByShard(shardID)
		wg.Add(1)

		var shardWg sync.WaitGroup
		ts0ShardQuorumCnt := 0

		for replicaPid := range replicaPids {
			shardWg.Add(1)

			go func(shardID, rpid, replicasCount int) {
				defer shardWg.Done()

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

				mu.Lock()
				defer mu.Unlock()

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
			}(shardID, replicaPid, len(replicaPids))
		}

		go func() {
			shardWg.Wait()

			wg.Done()
		}()
	}

	wg.Wait()

	tsCommit := ts0

	// Check for fast-path quorums
	if len(ts0PerShardQuorums) != len(shardToKeys) {
		// No fast-path quorum; perform second round-trip

		tsCommit = proposedMax
		shardToDeps = map[int][]*proto.Transaction{}

		wg := sync.WaitGroup{}
		mu := sync.Mutex{}

		for shardID, keys := range shardToKeys {
			replicaPids := c.env.ReplicaPidsByShard(shardID)
			wg.Add(1)

			var shardWg sync.WaitGroup

			for replicaPid := range replicaPids {
				shardWg.Add(1)

				go func(shardID, rpid int) {
					defer shardWg.Done()

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

					mu.Lock()
					defer mu.Unlock()

					shardToDeps[shardID] = append(shardToDeps[shardID], resp.Deps...)
				}(shardID, replicaPid)
			}

			go func() {
				shardWg.Wait()

				wg.Done()
			}()
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

	wg = sync.WaitGroup{}
	mu = sync.Mutex{}

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

	result, writes, err := c.queryExecutor.Execute(query, allReads)
	if err != nil {
		return "", fmt.Errorf("cannot execute query: %w", err)
	}

	for shardID := range shardToKeys {
		replicaPids := c.env.ReplicaPidsByShard(shardID)

		for replicaPid := range replicaPids {
			go func(shardID, rpid int) {
				d := shardToDeps[shardID]

				req := &proto.ApplyRequest{
					Txn:    txn,
					Ts:     tsCommit,
					Deps:   d,
					Result: writes,
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

	return result, nil
}
