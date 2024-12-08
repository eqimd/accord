package cmd

import (
	"net"
	"os"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/ports/rpc"
	"github.com/eqimd/accord/internal/storage"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(replicaCmd)
}

var replicaCmd = &cobra.Command{
	Use:   "replica",
	Short: "Run as replica",
	RunE: func(cmd *cobra.Command, args []string) error {
		addr := args[0]
		storage := storage.NewInMemory()

		replica := cluster.NewReplica(os.Getpid(), storage)

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}

		grpcServer := grpc.NewServer()
		rpc.RegisterReplicaServer(grpcServer, rpc.NewReplicaServer(replica))

		return grpcServer.Serve(lis)
	},
}
