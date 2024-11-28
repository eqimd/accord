package cluster

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/eqimd/accord/internal/cluster/provider"
	"github.com/eqimd/accord/internal/message"
)

type monoClock struct {
	val atomic.Uint64
}

func (c *monoClock) getTime() uint64 {
	v := c.val.Add(1)

	return v
}

type Coordinator struct {
	pid           int
	env           provider.Environment
	sharding      provider.Sharding
	queryExecutor provider.QueryExecutor

	clock monoClock
}

func NewCoordinator(
	pid int,
	env provider.Environment,
	sharding provider.Sharding,
	queryExecutor provider.QueryExecutor,
) *Coordinator {
	return &Coordinator{
		pid:           pid,
		env:           env,
		sharding:      sharding,
		queryExecutor: queryExecutor,
	}
}

func (c *Coordinator) Exec(query string) (string, error) {
	ts0 := message.Timestamp{
		LocalTime:   c.clock.getTime(),
		LogicalTime: 0,
		Pid:         c.pid,
	}

	keys, err := c.queryExecutor.QueryKeys(query)
	if err != nil {
		slog.Error("keys error")
		// TODO
	}

	shardToKeys := c.sharding.ShardToKeys(keys)

	deps := []message.Transaction{}
	shardToDeps := map[int][]message.Transaction{}

	var proposedMax message.Timestamp
	ts0PerShardQuorums := map[int]struct{}{}

	var wg sync.WaitGroup
	var mu sync.Mutex

	txn := message.Transaction{
		TxnHash:   query,
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

				propTs, rDeps, err := c.env.PreAccept(c.pid, rpid, txn, keys, ts0)
				if err != nil {
					slog.Error("preaccept error", slog.Any("error", err))
					// TODO
				}

				mu.Lock()
				defer mu.Unlock()

				if propTs.Equal(ts0) {
					ts0ShardQuorumCnt++

					if 2*ts0ShardQuorumCnt > replicasCount {
						ts0PerShardQuorums[shardID] = struct{}{}
					}
				}

				if proposedMax.Less(propTs) {
					proposedMax = propTs
				}

				deps = append(deps, rDeps.Deps...)

				shardToDeps[shardID] = append(shardToDeps[shardID], rDeps.Deps...)
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
		deps = []message.Transaction{}
		shardToDeps = map[int][]message.Transaction{}

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

					tDeps, err := c.env.Accept(c.pid, rpid, txn, keys, tsCommit)
					if err != nil {
						slog.Error("accept error", slog.Any("error", err))
						// TODO
					}

					mu.Lock()
					defer mu.Unlock()

					deps = append(deps, tDeps.Deps...)

					shardToDeps[shardID] = append(shardToDeps[shardID], tDeps.Deps...)
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
				err := c.env.Commit(c.pid, rpid, txn)
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
		for k := range c.env.ReplicaPidsByShard(shardID) {
			replicaPid = k
			break
		}

		shardDeps := message.TxnDependencies{
			Deps: shardToDeps[shardID],
		}

		keys := keys

		go func() {
			defer wg.Done()

			reads, err := c.env.Read(
				c.pid,
				replicaPid,
				txn,
				keys,
				tsCommit,
				shardDeps,
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
				d := message.TxnDependencies{
					Deps: shardToDeps[shardID],
				}

				err := c.env.Apply(c.pid, rpid, txn, tsCommit, d, writes)
				if err != nil {
					slog.Error("apply error", slog.Any("error", err))
					// TODO
				}
			}(shardID, replicaPid)
		}
	}

	return result, nil
}
