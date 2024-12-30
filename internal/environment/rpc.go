package environment

import (
	"context"
	"sync"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcClient struct {
	conn   *grpc.ClientConn
	client proto.ReplicaClient
}

type GRPCEnv struct {
	mu sync.RWMutex

	replicaToAddr   map[int]string
	shardToReplicas map[int][]int
	replicaToClient map[int]*rpcClient

	curReplica *cluster.Replica
}

func NewGRPCEnv(
	replicaAddrToShard map[string]int,
	curReplica *cluster.Replica,
	curAddr string,
	curPid int,
) (*GRPCEnv, error) {
	env := &GRPCEnv{
		replicaToAddr:   make(map[int]string),
		shardToReplicas: make(map[int][]int),
		replicaToClient: make(map[int]*rpcClient),
		curReplica:      curReplica,
	}

	for addr, shard := range replicaAddrToShard {
		if addr == curAddr {
			env.mu.Lock()

			env.replicaToAddr[curPid] = addr
			env.shardToReplicas[shard] = append(env.shardToReplicas[shard], curPid)

			env.mu.Unlock()

			continue
		}

		addr := addr
		shard := shard

		go func() {
			for {
				conn, err := grpc.NewClient(
					addr,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					continue
				}

				client := proto.NewReplicaClient(conn)
				resp, err := client.Pid(
					context.Background(),
					&proto.PidRequest{},
				)
				if err != nil {
					continue
				}

				pid := int(*resp.Pid)

				env.mu.Lock()

				env.replicaToClient[pid] = &rpcClient{
					conn:   conn,
					client: client,
				}

				env.replicaToAddr[pid] = addr
				env.shardToReplicas[shard] = append(env.shardToReplicas[shard], pid)

				env.mu.Unlock()

				break
			}
		}()
	}

	return env, nil
}

// TODO add context to env

func (e *GRPCEnv) PreAccept(
	from, to int,
	req *proto.PreAcceptRequest,
) (*proto.PreAcceptResponse, error) {
	if from == to {
		return e.curReplica.PreAccept(
			from,
			req,
		)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	resp, err := e.replicaToClient[to].client.PreAccept(
		context.Background(),
		req,
	)

	return resp, err
}

func (e *GRPCEnv) Accept(
	from, to int,
	req *proto.AcceptRequest,
) (*proto.AcceptResponse, error) {
	if from == to {
		return e.curReplica.Accept(
			from,
			req,
		)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	resp, err := e.replicaToClient[to].client.Accept(
		context.Background(),
		req,
	)

	return resp, err
}

func (e *GRPCEnv) Commit(
	from, to int,
	req *proto.CommitRequest,
) error {
	if from == to {
		return e.curReplica.Commit(
			from,
			req,
		)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	_, err := e.replicaToClient[to].client.Commit(
		context.Background(),
		req,
	)

	return err
}

func (e *GRPCEnv) Read(
	from, to int,
	req *proto.ReadRequest,
) (map[string]string, error) {
	if from == to {
		return e.curReplica.Read(
			from,
			req,
		)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	resp, err := e.replicaToClient[to].client.Read(
		context.Background(),
		req,
	)

	return resp.Reads, err
}

func (e *GRPCEnv) Apply(
	from, to int,
	req *proto.ApplyRequest,
) error {
	if from == to {
		return e.curReplica.Apply(
			from,
			req,
		)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	_, err := e.replicaToClient[to].client.Apply(
		context.Background(),
		req,
	)

	return err
}

func (e *GRPCEnv) ReplicaPidsByShard(shardID int) common.Set[int] {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return common.SetFromSlice(e.shardToReplicas[shardID])
}
