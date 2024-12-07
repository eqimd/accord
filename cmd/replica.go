package cmd

import (
	"net/http"
	"os"
	"time"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/ports"
	"github.com/eqimd/accord/internal/storage"
	"github.com/spf13/cobra"
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

		handler := ports.NewReplicaHandler(replica)

		server := &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  10 * time.Second,
		}

		return server.ListenAndServe()
	},
}
