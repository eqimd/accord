package main

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/eqimd/accord/proto"
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

	rpcClients := []proto.CoordinatorClient{}

	for _, addr := range coordinators {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}

		client := proto.NewCoordinatorClient(conn)

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

			// st := seededRand.Intn(100)

			// time.Sleep(time.Duration(st) * time.Millisecond)

			pos1 := rand.Int() % len(keys)

			key1 := keys[pos1]

			val1 := RandomString(10)

			coordPos := seededRand.Intn(len(coordinators))
			coordClient := rpcClients[coordPos]

			err := coordinatorExec(coordClient, key1, val1)
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

	snapshotAll, err := rpcClients[0].Snapshot(context.Background(), &proto.SnapshotAllRequest{})
	if err != nil {
		panic(err)
	}

	for _, replicas := range snapshotAll.Shards {
		var repRand int32
		for rPid := range replicas.Replicas {
			repRand = rPid
		}

		for _, srep := range replicas.Replicas {
			if !maps.Equal(srep.Values, replicas.Replicas[repRand].Values) {
				panic(fmt.Sprintf("not equal maps\n%v\n%v", srep.Values, replicas.Replicas[repRand].Values))
			}
		}
	}
}

func coordinatorExec(client proto.CoordinatorClient, key, val string) error {
	query := fmt.Sprintf("let x = SET(\"%s\", \"%s\"); x", key, val)
	_, err := client.Execute(
		context.Background(),
		&proto.ExecuteRequest{
			Query: &query,
		},
	)
	// _, err := client.Put(
	// 	context.Background(),
	// 	&proto.PutRequest{
	// 		Vals: map[string]string{
	// 			key: val,
	// 		},
	// 	},
	// )

	return err
}

func TestMain(t *testing.T) {
	start := time.Now()

	runTest()

	end := time.Now()

	fmt.Println("Time:", end.Sub(start))
}
