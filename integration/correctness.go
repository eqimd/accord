package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/eqimd/accord/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var correctnessCmd = &cobra.Command{
	Use:   "correctness runsCount keysCount [addresses]",
	Short: "Run correctness test",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 3 {
			return fmt.Errorf("less than 3 args")
		}

		runsCount, err := strconv.Atoi(args[0])
		if err != nil {
			return err
		}

		keysCount, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}

		runCorrectness(runsCount, keysCount, args[2:])

		return nil
	},
}

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

func runCorrectness(runsCount, keysCount int, addresses []string) {
	coordinators := addresses

	rpcClients := []proto.CoordinatorClient{}

	for _, addr := range coordinators {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}

		client := proto.NewCoordinatorClient(conn)

		rpcClients = append(rpcClients, client)
	}

	keys := make([]string, 0, keysCount)
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

	fmt.Println("Time:", end.Sub(start))

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

		for rpid, srep := range replicas.Replicas {
			for k, v := range srep.Values {
				rv := replicas.Replicas[repRand].Values[k]
				if rv != v {
					fmt.Printf("Diff on %d and %d, key %s, values %s %s\n", repRand, rpid, k, rv, v)
				}
			}
			// if !maps.Equal(srep.Values, replicas.Replicas[repRand].Values) {
			// 	panic(fmt.Sprintf("not equal maps\n%v\n%v", srep.Values, replicas.Replicas[repRand].Values))
			// }
		}
	}
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
