package rpc

import (
	"context"

	"github.com/eqimd/accord/internal/coordinator"
	"github.com/eqimd/accord/proto"
)

type coordinatorServer struct {
	proto.UnimplementedCoordinatorServer

	coordinator *coordinator.Coordinator
}

func NewCoordinatorServer(coordinator *coordinator.Coordinator) *coordinatorServer {
	return &coordinatorServer{
		coordinator: coordinator,
	}
}

func (s *coordinatorServer) Execute(ctx context.Context, req *proto.ExecuteRequest) (*proto.ExecuteResponse, error) {
	res, err := s.coordinator.Exec(*req.Query)

	return &proto.ExecuteResponse{Result: &res}, err
}

func (s *coordinatorServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	err := s.coordinator.Put(req.Vals)

	return &proto.PutResponse{}, err
}

func (s *coordinatorServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	reads, err := s.coordinator.Get(req.Keys)

	return &proto.GetResponse{
		Result: reads,
	}, err
}
