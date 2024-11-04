package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(replicaCmd)
}

var replicaCmd = &cobra.Command{
	Use:   "replica",
	Short: "Run as replica",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("replica")
		return nil
	},
}
