package cmd

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/eqimd/accord/cmd/config"
	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/discovery"
	"github.com/eqimd/accord/internal/environment"
	"github.com/eqimd/accord/internal/ports"
	"github.com/eqimd/accord/internal/query"
	"github.com/eqimd/accord/internal/sharding"
	"github.com/spf13/cobra"
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

		addrs := make([]string, 0, len(config.Replicas))
		shards := common.Set[int]{}

		for _, r := range config.Replicas {
			addrs = append(addrs, r.Address)
			shards.Add(r.ShardID)
		}

		discoveryPids, err := discovery.DiscoverReplicas(addrs)
		if err != nil {
			return err
		}

		shardToPids := map[int][]int{}
		replicaToAddr := map[int]string{}
		for _, r := range config.Replicas {
			pid := discoveryPids.AddrToPid[r.Address]

			shardToPids[r.ShardID] = append(shardToPids[r.ShardID], pid)
			replicaToAddr[pid] = r.Address
		}

		env := environment.NewHTTP(shardToPids, replicaToAddr)
		shrd := sharding.NewHash(shards)
		qexecutor := query.NewExecutor()

		coordinator := cluster.NewCoordinator(
			os.Getpid(),
			env,
			shrd,
			qexecutor,
		)

		handler := ports.NewCoordinatorHandler(coordinator)

		server := &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  5 * time.Minute,
			WriteTimeout: 5 * time.Minute,
			IdleTimeout:  5 * time.Minute,
		}

		return server.ListenAndServe()
	},
}
