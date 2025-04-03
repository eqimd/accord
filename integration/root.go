package main

import (
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(correctnessCmd)
}

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "Run accord tests",
}

func execute() error {
	return rootCmd.Execute()
}
