package main

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/environment"
	"github.com/eqimd/accord/internal/ports/rpc"
	"github.com/eqimd/accord/internal/query"
	"github.com/eqimd/accord/internal/sharding"
	"github.com/eqimd/accord/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func RandomStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandomString(length int) string {
	return RandomStringWithCharset(length, charset)
}

func runTest() {
	coordinators := []string{
		"localhost:5000",
		"localhost:6000",
		"localhost:7000",
	}

	rpcClients := []rpc.CoordinatorClient{}

	for _, addr := range coordinators {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}

		client := rpc.NewCoordinatorClient(conn)

		rpcClients = append(rpcClients, client)
	}

	runsCount := 10000

	keys := make([]string, 0, 1000)
	for range cap(keys) {
		keys = append(keys, RandomString(10))
	}

	var wg sync.WaitGroup

	start := time.Now()

	for range runsCount {
		wg.Add(1)

		go func() {
			defer wg.Done()

			pos1 := rand.Int() % len(keys)

			key1 := keys[pos1]

			val1 := RandomString(10)

			q := fmt.Sprintf(`let val1 = SET("%s", "%s"); val1`, key1, val1)

			coordPos := rand.Int() % len(coordinators)
			coordClient := rpcClients[coordPos]

			_, err := coordinatorExec(coordClient, q)
			if err != nil {
				panic(err)
			}
		}()
	}

	wg.Wait()

	end := time.Now()

	qps := float64(runsCount) / (float64(end.Sub(start)) / float64(time.Second))
	fmt.Println("QPS:", qps)

	time.Sleep(10 * time.Second)

	fmt.Println()
}

func coordinatorExec(client rpc.CoordinatorClient, query string) (string, error) {
	resp, err := client.Execute(
		context.Background(),
		&rpc.ExecuteRequest{
			Query: &query,
		},
	)
	if err != nil {
		return "", err
	}

	return *resp.Result, nil
}

func TestMain(t *testing.T) {
	start := time.Now()

	runTest()

	end := time.Now()

	fmt.Println(end.Sub(start))
}

func TestLocal(t *testing.T) {
	for {
		shardsCount := 1
		replicasPerShard := 3
		runsCount := 500

		shardIDs := common.Set[int]{}
		shardToReplicas := map[int]map[int]*cluster.Replica{}
		replicaToStorage := map[int]*storage.InMemory{}

		repID := 0

		for sh := range shardsCount {
			shardIDs.Add(sh)
			shardToReplicas[sh] = map[int]*cluster.Replica{}

			for range replicasPerShard {
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

		keys := make([]string, 0, 10)
		for range cap(keys) {
			keys = append(keys, RandomString(10))
		}

		resCh := make(chan string)

		var wg sync.WaitGroup

		for range runsCount {
			wg.Add(1)

			go func() {
				defer wg.Done()

				pos1 := rand.Int() % len(keys)

				key1 := keys[pos1]

				val1 := RandomString(10)

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

		time.Sleep(3 * time.Second)

		// for _, key := range keys {
		// 	q := fmt.Sprintf("let val = GET(\"%s\"); val", key)
		// 	res, _ := coordinators[coordOffset].Exec(q)
		//
		// fmt.Println(key, "=", res)
		// }

		// fmt.Println()

		snapshot := map[int]map[string]string{}

		for rPid, strg := range replicaToStorage {
			snps, _ := strg.Snapshot()
			snapshot[rPid] = snps
		}

		// for i, snsh := range snapshot {
		// 	fmt.Println("pid", i)
		// 	fmt.Println(snsh)
		// 	fmt.Println()
		// }

		for _, replicas := range shardToReplicas {
			var pid1 int
			for rpid := range replicas {
				pid1 = rpid
				break
			}

			snsh := snapshot[pid1]

			for rPid := range replicas {
				if !maps.Equal(snsh, snapshot[rPid]) {
					panic("mda")
				}
				// require.Equal(t, snsh, snapshot[rPid])
			}
		}
	}
}
