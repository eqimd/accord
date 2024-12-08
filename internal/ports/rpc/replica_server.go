package rpc

import (
	"context"
	"fmt"

	"github.com/eqimd/accord/internal/cluster"
)

type replicaServer struct {
	UnimplementedReplicaServer

	replica *cluster.Replica
}

func NewReplicaServer(replica *cluster.Replica) *replicaServer {
	return &replicaServer{
		replica: replica,
	}
}

func (s *replicaServer) PreAccept(ctx context.Context, req *PreAcceptRequest) (*PreAcceptResponse, error) {
	ts, deps, err := s.replica.PreAccept(
		int(*req.Sender),
		TxnFromGrpc(req.Txn),
		req.Keys,
		TsFromGrpc(req.Ts0),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot pre accept: %w", err)
	}

	resp := &PreAcceptResponse{
		Ts:   TsToGrpc(&ts),
		Deps: DepsToGrpc(&deps),
	}

	return resp, nil
}

func (s *replicaServer) Accept(ctx context.Context, req *AcceptRequest) (*AcceptResponse, error) {
	deps, err := s.replica.Accept(
		int(*req.Sender),
		TxnFromGrpc(req.Txn),
		req.Keys,
		TsFromGrpc(req.Ts),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot accept: %w", err)
	}

	return &AcceptResponse{Deps: DepsToGrpc(&deps)}, nil
}

func (s *replicaServer) Commit(ctx context.Context, req *CommitRequest) (*CommitResponse, error) {
	err := s.replica.Commit(
		int(*req.Sender),
		TxnFromGrpc(req.Txn),
		TsFromGrpc(req.Ts),
	)

	return &CommitResponse{}, err
}

func (s *replicaServer) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	reads, err := s.replica.Read(
		int(*req.Sender),
		TxnFromGrpc(req.Txn),
		req.Keys,
		TsFromGrpc(req.Ts),
		DepsFromGrpc(req.Deps),
	)

	return &ReadResponse{Reads: reads}, err
}

func (s *replicaServer) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	err := s.replica.Apply(
		int(*req.Sender),
		TxnFromGrpc(req.Txn),
		TsFromGrpc(req.Ts),
		DepsFromGrpc(req.Deps),
		req.Result,
	)

	return &ApplyResponse{}, err
}

func (s *replicaServer) Pid(ctx context.Context, req *PidRequest) (*PidResponse, error) {
	pid := int32(s.replica.Pid())

	return &PidResponse{Pid: &pid}, nil
}

func (s *replicaServer) Snapshot(ctx context.Context, req *SnapshotRequest) (*SnapshotResponse, error) {
	snapshot, err := s.replica.Snapshot()

	return &SnapshotResponse{Result: snapshot}, err
}
