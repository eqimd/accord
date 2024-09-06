package cluster

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/eqimd/accord/common"
	"github.com/eqimd/accord/message"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
)

type Coordinator struct {
	pid int
	env *Environment
}

func NewCoordinator(
	pid int,
	env *Environment,
) *Coordinator {
	return &Coordinator{
		pid: pid,
		env: env,
	}
}

func (c *Coordinator) Exec(query string) (string, error) {
	ts0 := message.Timestamp{
		LocalTime:   time.Now(),
		LogicalTime: 0,
		Pid:         c.pid,
	}

	shardToKeys, err := c.getQueryShardToKeys(query)
	if err != nil {
		return "", fmt.Errorf("cannot exec query: %w", err)
	}

	deps := message.TxnDependencies{
		Deps: make(common.Set[message.Transaction]),
	}
	shardToDeps := map[int]common.Set[message.Transaction]{}

	var proposedMax message.Timestamp
	ts0PerShardQuorums := map[int]struct{}{}

	var wg sync.WaitGroup
	var mu sync.Mutex

	txn := message.Transaction{
		TxnHash:   query,
		Timestamp: ts0,
	}

	for shardID, keys := range shardToKeys {
		replicaPids := c.getReplicaPidsByShard(shardID)
		wg.Add(1)

		var shardWg sync.WaitGroup
		ts0ShardQuorumCnt := 0

		for replicaPid := range replicaPids {
			shardWg.Add(1)

			go func(shardID, rpid int) {
				defer shardWg.Done()

				propTs, rDeps, _ := c.env.PreAccept(c.pid, rpid, txn, keys, ts0)

				mu.Lock()
				defer mu.Unlock()

				if propTs.Equal(ts0) {
					ts0ShardQuorumCnt++

					if 2*ts0ShardQuorumCnt > c.env.ReplicasPerShard {
						ts0PerShardQuorums[shardID] = struct{}{}
					}
				}

				if proposedMax.Less(propTs) {
					proposedMax = propTs
				}

				deps.Deps.Union(rDeps.Deps)

				if _, ok := shardToDeps[shardID]; !ok {
					shardToDeps[shardID] = make(common.Set[message.Transaction])
				}

				shardToDeps[shardID].Union(rDeps.Deps)
			}(shardID, replicaPid)
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
		deps.Deps = common.Set[message.Transaction]{}
		shardToDeps = map[int]common.Set[message.Transaction]{}

		wg := sync.WaitGroup{}
		mu := sync.Mutex{}

		for shardID, keys := range shardToKeys {
			replicaPids := c.getReplicaPidsByShard(shardID)
			wg.Add(1)

			var shardWg sync.WaitGroup

			for replicaPid := range replicaPids {
				shardWg.Add(1)

				go func(shardID, rpid int) {
					defer shardWg.Done()

					tDeps, _ := c.env.Accept(c.pid, rpid, txn, keys, ts0, proposedMax)

					mu.Lock()
					defer mu.Unlock()

					deps.Deps.Union(tDeps.Deps)

					if _, ok := shardToDeps[shardID]; !ok {
						shardToDeps[shardID] = make(common.Set[message.Transaction])
					}

					shardToDeps[shardID].Union(tDeps.Deps)
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
		replicaPids := c.getReplicaPidsByShard(shardID)
		for replicaPid := range replicaPids {
			go func(rpid int) {
				_ = c.env.Commit(c.pid, rpid, txn, ts0, tsCommit, deps)
			}(replicaPid)
		}
	}

	wg = sync.WaitGroup{}
	mu = sync.Mutex{}

	allReads := map[string]string{}

	for shardID, keys := range shardToKeys {
		wg.Add(1)

		// TODO Use shardID as replica pid ; need to fix this
		replicaPid := shardID * c.env.ReplicasPerShard

		shardDeps := message.TxnDependencies{
			Deps: shardToDeps[shardID],
		}

		keys := keys

		go func() {
			defer wg.Done()

			reads, _ := c.env.Read(
				c.pid,
				replicaPid,
				txn,
				keys,
				tsCommit,
				shardDeps,
			)
			mu.Lock()

			for k, v := range reads {
				allReads[k] = v
			}

			mu.Unlock()
		}()
	}

	wg.Wait()

	result, writes, err := c.execute(query, allReads)
	if err != nil {
		return "", fmt.Errorf("cannot execute query: %w", err)
	}

	for shardID := range shardToKeys {
		replicaPids := c.getReplicaPidsByShard(shardID)

		for replicaPid := range replicaPids {
			go func(shardID, rpid int) {
				d := message.TxnDependencies{
					Deps: shardToDeps[shardID],
				}

				_ = c.env.Apply(c.pid, rpid, txn, tsCommit, d, writes)
			}(shardID, replicaPid)
		}
	}

	return result, nil
}

func (c *Coordinator) getReplicaPidsByShard(shardID int) common.Set[int] {
	pids := common.Set[int]{}

	for i := 0; i < c.env.ReplicasPerShard; i++ {
		pid := (shardID * c.env.ReplicasPerShard) + i
		pids.Add(pid)
	}

	return pids
}

// returns mapping of (shard id) -> (set of keys for this shard)
func (c *Coordinator) getQueryShardToKeys(query string) (map[int]common.Set[string], error) {
	keys, err := getQueryKeys(query)
	if err != nil {
		return nil, fmt.Errorf("cannot get query keys: %w", err)
	}

	shardToKeys := map[int]common.Set[string]{}

	for key := range keys {
		shardID := getShardByKey(key, c.env.Shards)

		if _, ok := shardToKeys[shardID]; !ok {
			shardToKeys[shardID] = make(common.Set[string])
		}

		shardToKeys[shardID].Add(key)
	}

	return shardToKeys, nil
}

func (c *Coordinator) execute(
	query string,
	reads map[string]string,
) (string, map[string]string, error) {
	executor := &queryExecutor{
		preValues: reads,
		setValues: map[string]string{},
	}

	env := map[string]any{
		"GET": executor.get,
		"SET": executor.set,
	}

	result, err := expr.Eval(query, env)
	if err != nil {
		return "", nil, fmt.Errorf("cannot execute query: %w", err)
	}

	return result.(string), executor.setValues, nil
}

func getShardByKey(key string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	return int(h.Sum32() % uint32(shards))
}

func getQueryKeys(query string) (common.Set[string], error) {
	keys := common.Set[string]{}

	visitor := &exprVisitor{}

	_, err := expr.Compile(query, expr.Patch(visitor))
	if err != nil {
		return nil, fmt.Errorf("query compile error: %w", err)
	}

	for _, s := range visitor.keys {
		keys.Add(s)
	}

	return keys, nil
}

type exprVisitor struct {
	prev string
	keys []string
}

func (v *exprVisitor) Visit(node *ast.Node) {
	cur := (*node).String()

	if v.prev == "GET" || v.prev == "SET" {
		cur = strings.Trim(cur, "\"")

		v.keys = append(v.keys, cur)
	}

	v.prev = cur
}

type queryExecutor struct {
	// key -> value mapping known before executing query; also used for further execution
	preValues map[string]string
	// values that should be set after execution
	setValues map[string]string
}

func (e *queryExecutor) get(key string) string {
	return e.preValues[key]
}

func (e *queryExecutor) set(key, value string) string {
	oldVal := e.preValues[key]

	e.preValues[key] = value
	e.setValues[key] = value

	return oldVal
}
