package rpc

import (
	"context"

	"github.com/eqimd/accord/internal/cluster"
)

type coordinatorServer struct {
	UnimplementedCoordinatorServer

	coordinator *cluster.Coordinator
}

func NewCoordinatorServer(coordinator *cluster.Coordinator) *coordinatorServer {
	return &coordinatorServer{
		coordinator: coordinator,
	}
}

func (s *coordinatorServer) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	res, err := s.coordinator.Exec(*req.Query)

	return &ExecuteResponse{Result: &res}, err
}
