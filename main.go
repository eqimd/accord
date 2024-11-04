package main

import (
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"time"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/environment"
	"github.com/eqimd/accord/internal/query"
	"github.com/eqimd/accord/internal/sharding"
	"github.com/eqimd/accord/internal/storage"
)

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func main() {
	for range 1 {
		runTest()
	}
}

func runTest() {
	shardsCount := 3
	replicasPerShard := 3
	runsCount := 10000

	shardIDs := common.Set[int]{}
	shardToReplicas := map[int]map[int]*cluster.Replica{}
	replicaToStorage := map[int]*storage.InMemory{}

	repID := 0

	for sh := range shardsCount {
		shardIDs.Add(sh)
		shardToReplicas[sh] = map[int]*cluster.Replica{}

		for _ = range replicasPerShard {
			strg := storage.NewInMemory()

			shardToReplicas[sh][repID] = cluster.NewReplica(repID, strg)
			replicaToStorage[repID] = strg

			repID++
		}
	}

	environment := environment.NewLocal(shardToReplicas)
	queryExecutor := query.NewExecutor()
	hashSharding := sharding.NewHash(shardIDs)

	coordinators := map[int]*cluster.Coordinator{}
	coordOffset := repID

	for range shardsCount {
		coordinators[repID] = cluster.NewCoordinator(repID, environment, hashSharding, queryExecutor)

		repID++
	}

	keys := make([]string, 0, 5)
	for range cap(keys) {
		keys = append(keys, String(10))
	}

	resCh := make(chan string)

	var wg sync.WaitGroup

	for range runsCount {
		wg.Add(1)

		go func() {
			defer wg.Done()

			pos1 := rand.Int() % len(keys)

			key1 := keys[pos1]

			val1 := String(10)

			q := fmt.Sprintf(`let val1 = SET("%s", "%s"); val1`, key1, val1)

			coordinatorPid := coordOffset + (rand.Int() % shardsCount)

			result, err := coordinators[coordinatorPid].Exec(q)
			if err != nil {
				panic(err)
			}

			resCh <- result
		}()
	}

	go func() {
		wg.Wait()

		close(resCh)
	}()

	for s := range resCh {
		_ = s
		// fmt.Println(s)
	}

	time.Sleep(10 * time.Second)

	for _, key := range keys {
		q := fmt.Sprintf("let val = GET(\"%s\"); val", key)
		res, _ := coordinators[coordOffset].Exec(q)

		fmt.Println(key, "=", res)
	}

	fmt.Println()

	snapshot := map[int]map[string]string{}

	for rPid, strg := range replicaToStorage {
		snps, _ := strg.Snapshot()
		snapshot[rPid] = snps
	}

	for i, snsh := range snapshot {
		fmt.Println("pid", i)
		fmt.Println(snsh, "\n")
	}

	for _, replicas := range shardToReplicas {
		var pid1 int
		for rpid := range replicas {
			pid1 = rpid
			break
		}

		snsh := snapshot[pid1]

		for rPid := range replicas {
			if !maps.Equal(snsh, snapshot[rPid]) {
				panic(fmt.Sprintf("not equal maps: pid1 = %d, pid2 = %d, map1 = %v, map2 = %v", pid1, rPid, snsh, snapshot[rPid]))
			}
		}
	}
}

// func main() {
// 	if err := cmd.Execute(); err != nil {
// 		fmt.Println("error:", err.Error())
// 		os.Exit(1)
// 	}
// }
