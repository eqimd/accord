package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(coordinatorCmd)
}

var coordinatorCmd = &cobra.Command{
	Use:   "coordinator",
	Short: "Run as coordinator",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("coord")

		return nil
	},
}
