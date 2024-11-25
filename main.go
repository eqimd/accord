package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/eqimd/accord/cmd"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	if err := cmd.Execute(); err != nil {
		fmt.Println("error:", err.Error())
		os.Exit(1)
	}
}
