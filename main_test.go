package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/eqimd/accord/internal/ports/model"
	"github.com/go-resty/resty/v2"
)

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func RandomStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandomString(length int) string {
	return RandomStringWithCharset(length, charset)
}

func runTest() {
	coordinators := []string{
		"http://localhost:5000",
		"http://localhost:6000",
		"http://localhost:7000",
	}

	runsCount := 10000

	keys := make([]string, 0, 10000)
	for range cap(keys) {
		keys = append(keys, RandomString(10))
	}

	resCh := make(chan string)

	var wg sync.WaitGroup

	start := time.Now()

	for range runsCount {
		wg.Add(1)

		go func() {
			defer wg.Done()

			pos1 := rand.Int() % len(keys)

			key1 := keys[pos1]

			val1 := RandomString(10)

			q := fmt.Sprintf(`let val1 = SET("%s", "%s"); val1`, key1, val1)

			coordPos := rand.Int() % len(coordinators)
			coordAddr := coordinators[coordPos]

			result, err := coordinatorExec(coordAddr, q)
			if err != nil {
				panic(err)
			}

			resCh <- result
		}()
	}

	go func() {
		wg.Wait()

		close(resCh)
	}()

	for s := range resCh {
		_ = s
		// fmt.Println(s)
	}

	end := time.Now()

	rps := float64(runsCount) / (float64(end.Sub(start)) / float64(time.Second))
	fmt.Println("RPS:", rps)

	time.Sleep(10 * time.Second)

	for _, key := range keys {
		q := fmt.Sprintf("let val = GET(\"%s\"); val", key)
		res, _ := coordinatorExec(coordinators[0], q)

		fmt.Println(key, "=", res)
	}

	fmt.Println()
}

func coordinatorExec(addr string, query string) (string, error) {
	client := resty.New()
	client.BaseURL = addr
	client.SetTimeout(5 * time.Minute)

	req := &model.ExecuteRequest{
		Query: query,
	}

	var resp model.ExecuteResponse

	rr, err := client.R().SetBody(req).SetResult(&resp).Post("/execute")
	if err != nil {
		return "", err
	}

	if rr.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("error: %s", rr.Body())
	}

	return resp.Response, nil
}

func TestMain(t *testing.T) {
	start := time.Now()

	runTest()

	end := time.Now()

	fmt.Println(end.Sub(start))
}
