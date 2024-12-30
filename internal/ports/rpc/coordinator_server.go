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
