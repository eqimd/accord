package main

import (
	"fmt"
	"os"
)

func main() {
	if err := execute(); err != nil {
		fmt.Println("error:", err.Error())
		os.Exit(1)
	}
}
