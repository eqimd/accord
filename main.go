package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/pprof"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/eqimd/accord/cmd"
)

func main() {
	go func() {
		_ = http.ListenAndServe(":6007", nil)
	}()

	f, err := os.Create("profile.prof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(10 * time.Second)
		pprof.StopCPUProfile()
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
