package environment

import (
	"context"
	"fmt"
	"sync"

	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
	"github.com/eqimd/accord/internal/ports/rpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcClient struct {
	conn   *grpc.ClientConn
	client rpc.ReplicaClient
}

type GRPCEnv struct {
	replicaToAddr   map[int]string
	shardToReplicas map[int][]int
	replicaToClient map[int]*rpcClient
}

func NewGRPCEnv(replicaAddrToShard map[string]int) (*GRPCEnv, error) {
	env := &GRPCEnv{
		replicaToAddr:   make(map[int]string),
		shardToReplicas: make(map[int][]int),
		replicaToClient: make(map[int]*rpcClient),
	}

	var mu sync.Mutex
	var errGroup errgroup.Group

	for addr, shard := range replicaAddrToShard {
		addr := addr
		shard := shard

		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}

		client := rpc.NewReplicaClient(conn)

		errGroup.Go(func() error {
			resp, err := client.Pid(
				context.Background(),
				&rpc.PidRequest{},
			)
			if err != nil {
				return fmt.Errorf("error getting pid from %s: %w", addr, err)
			}

			pid := int(*resp.Pid)

			mu.Lock()

			env.replicaToClient[pid] = &rpcClient{
				conn:   conn,
				client: client,
			}

			env.replicaToAddr[pid] = addr
			env.shardToReplicas[shard] = append(env.shardToReplicas[shard], pid)

			mu.Unlock()

			return nil
		})
	}

	err := errGroup.Wait()

	return env, err
}

// TODO add context to env

func (e *GRPCEnv) PreAccept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts0 message.Timestamp,
) (message.Timestamp, message.TxnDependencies, error) {
	sender := int32(from)

	resp, err := e.replicaToClient[to].client.PreAccept(
		context.Background(),
		&rpc.PreAcceptRequest{
			Txn:    rpc.TxnToGrpc(&txn),
			Keys:   keys,
			Ts0:    rpc.TsToGrpc(&ts0),
			Sender: &sender,
		},
	)
	if err != nil {
		return message.Timestamp{}, message.TxnDependencies{}, err
	}

	return rpc.TsFromGrpc(resp.Ts), rpc.DepsFromGrpc(resp.Deps), err
}

func (e *GRPCEnv) Accept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	sender := int32(from)

	resp, err := e.replicaToClient[to].client.Accept(
		context.Background(),
		&rpc.AcceptRequest{
			Txn:    rpc.TxnToGrpc(&txn),
			Keys:   keys,
			Ts:     rpc.TsToGrpc(&ts),
			Sender: &sender,
		},
	)
	if err != nil {
		return message.TxnDependencies{}, err
	}

	return rpc.DepsFromGrpc(resp.Deps), nil
}

func (e *GRPCEnv) Commit(
	from, to int,
	txn message.Transaction,
	ts message.Timestamp,
) error {
	sender := int32(from)

	_, err := e.replicaToClient[to].client.Commit(
		context.Background(),
		&rpc.CommitRequest{
			Txn:    rpc.TxnToGrpc(&txn),
			Ts:     rpc.TsToGrpc(&ts),
			Sender: &sender,
		},
	)

	return err
}

func (e *GRPCEnv) Read(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
	deps message.TxnDependencies,
) (map[string]string, error) {
	sender := int32(from)

	resp, err := e.replicaToClient[to].client.Read(
		context.Background(),
		&rpc.ReadRequest{
			Txn:    rpc.TxnToGrpc(&txn),
			Keys:   keys,
			Ts:     rpc.TsToGrpc(&ts),
			Deps:   rpc.DepsToGrpc(&deps),
			Sender: &sender,
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.Reads, nil
}

func (e *GRPCEnv) Apply(
	from, to int,
	txn message.Transaction,
	ts message.Timestamp,
	deps message.TxnDependencies,
	result map[string]string,
) error {
	sender := int32(from)

	_, err := e.replicaToClient[to].client.Apply(
		context.Background(),
		&rpc.ApplyRequest{
			Txn:    rpc.TxnToGrpc(&txn),
			Ts:     rpc.TsToGrpc(&ts),
			Deps:   rpc.DepsToGrpc(&deps),
			Result: result,
			Sender: &sender,
		},
	)

	return err
}

func (e *GRPCEnv) ReplicaPidsByShard(shardID int) common.Set[int] {
	return common.SetFromSlice(e.shardToReplicas[shardID])
}
