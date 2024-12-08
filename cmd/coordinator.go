package cmd

import (
	"encoding/json"
	"net"
	"os"

	"github.com/eqimd/accord/cmd/config"
	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/environment"
	"github.com/eqimd/accord/internal/ports/rpc"
	"github.com/eqimd/accord/internal/query"
	"github.com/eqimd/accord/internal/sharding"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(coordinatorCmd)
}

var coordinatorCmd = &cobra.Command{
	Use:   "coordinator",
	Short: "Run as coordinator",
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath := args[0]
		addr := args[1]

		b, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}

		var config config.Config
		if err := json.Unmarshal(b, &config); err != nil {
			return err
		}

		addrToShard := map[string]int{}
		shards := common.Set[int]{}

		for _, r := range config.Replicas {
			addrToShard[r.Address] = r.ShardID
			shards.Add(r.ShardID)
		}

		env, err := environment.NewGRPCEnv(addrToShard)
		if err != nil {
			return err
		}

		shrd := sharding.NewHash(shards)
		qexecutor := query.NewExecutor()

		coordinator := cluster.NewCoordinator(
			os.Getpid(),
			env,
			shrd,
			qexecutor,
		)

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}

		grpcServer := grpc.NewServer()
		rpc.RegisterCoordinatorServer(grpcServer, rpc.NewCoordinatorServer(coordinator))

		return grpcServer.Serve(lis)
	},
}
