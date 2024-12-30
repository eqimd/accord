package rpc

import (
	"context"
	"fmt"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/proto"
)

type replicaServer struct {
	proto.UnimplementedReplicaServer

	replica *cluster.Replica
}

func NewReplicaServer(replica *cluster.Replica) *replicaServer {
	return &replicaServer{
		replica: replica,
	}
}

func (s *replicaServer) PreAccept(ctx context.Context, req *proto.PreAcceptRequest) (*proto.PreAcceptResponse, error) {
	resp, err := s.replica.PreAccept(
		int(*req.Sender),
		req,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot pre accept: %w", err)
	}

	return resp, nil
}

func (s *replicaServer) Accept(ctx context.Context, req *proto.AcceptRequest) (*proto.AcceptResponse, error) {
	resp, err := s.replica.Accept(
		int(*req.Sender),
		req,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot accept: %w", err)
	}

	return resp, nil
}

func (s *replicaServer) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitResponse, error) {
	err := s.replica.Commit(
		int(*req.Sender),
		req,
	)

	return &proto.CommitResponse{}, err
}

func (s *replicaServer) Read(ctx context.Context, req *proto.ReadRequest) (*proto.ReadResponse, error) {
	reads, err := s.replica.Read(
		int(*req.Sender),
		req,
	)

	return &proto.ReadResponse{Reads: reads}, err
}

func (s *replicaServer) Apply(ctx context.Context, req *proto.ApplyRequest) (*proto.ApplyResponse, error) {
	err := s.replica.Apply(
		int(*req.Sender),
		req,
	)

	return &proto.ApplyResponse{}, err
}

func (s *replicaServer) Pid(ctx context.Context, req *proto.PidRequest) (*proto.PidResponse, error) {
	pid := int32(s.replica.Pid())

	return &proto.PidResponse{Pid: &pid}, nil
}

func (s *replicaServer) Snapshot(ctx context.Context, req *proto.SnapshotRequest) (*proto.SnapshotResponse, error) {
	snapshot, err := s.replica.Snapshot()

	return &proto.SnapshotResponse{Result: snapshot}, err
}
