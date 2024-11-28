package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/eqimd/accord/cmd"
)

func main() {
	go func() {
		_ = http.ListenAndServe(":6007", nil)
	}()

	http.DefaultClient.Timeout = 2 * time.Minute

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	if err := cmd.Execute(); err != nil {
		fmt.Println("error:", err.Error())
		os.Exit(1)
	}
}
