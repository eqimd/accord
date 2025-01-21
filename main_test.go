package main

import (
	"context"
	"fmt"
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
		// "localhost:7000",
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

	keys := make([]string, 0, 10000)
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
}

func coordinatorExec(client proto.CoordinatorClient, key, val string) error {
	_, err := client.Put(
		context.Background(),
		&proto.PutRequest{
			Vals: map[string]string{
				key: val,
			},
		},
	)

	return err
}

func TestMain(t *testing.T) {
	start := time.Now()

	runTest()

	end := time.Now()

	fmt.Println("Time:", end.Sub(start))
}
