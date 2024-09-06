package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/eqimd/accord/cluster"
)

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func main() {
	for range 100 {
		shardsCount := 2
		replicasPerShard := 2
		runsCount := 2

		cluster := cluster.NewCluster(shardsCount, replicasPerShard)

		keys := make([]string, 0, 1)
		for range 1 {
			keys = append(keys, String(10))
		}

		resCh := make(chan string)

		var wg sync.WaitGroup

		for range runsCount {
			wg.Add(1)

			go func() {
				defer wg.Done()

				pos1 := rand.Int() % len(keys)

				key1 := keys[pos1]

				val1 := String(10)

				q := fmt.Sprintf(`let val1 = SET("%s", "%s"); val1`, key1, val1)

				coordinatorPid := shardsCount*replicasPerShard + (rand.Int() % shardsCount)

				result, err := cluster.Exec(q, coordinatorPid)
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
			fmt.Println(s)
		}

		// _ = cluster.Snapshot()
	}
}

/*
func main() {
	shardsCount := 5
	replicasPerShard := 3

	cluster := cluster.NewCluster(shardsCount, replicasPerShard)

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("-> ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if text == "snapshot" {
			snapshot := cluster.Snapshot()
			fmt.Println(snapshot)

			continue
		}

		splitText := strings.Split(text, " ")
		if splitText[0] == "exec" {
			pid, _ := strconv.Atoi(splitText[1])
			query := strings.Join(splitText[2:], " ")

			result, err := cluster.Exec(query, pid)
			if err != nil {
				fmt.Println("error:", err.Error())
				continue
			}

			fmt.Println("result:", result)
		}
	}
}
*/

/*
func main() {
	flag.Parse()

	// Create services, ignoring configuration errors.
	a := New("a", time.Second*3)
	b := New("b", time.Second*2)
	c := New("c", time.Second*5)
	// Start services.
	ac := a.Start()
	bc := b.Start()
	cc := c.Start()
	// Setup signal handler
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT)

retryLoop:
	for retries := 2; retries >= 0; retries-- {
		// Wait for any service to fail, restart them a couple times.
		select {
		case err := <-ac:
			log.Printf("error: %v", err)
			if retries > 0 {
				ac = a.Start()
			}
		case err := <-bc:
			log.Printf("error: %v", err)
			if retries > 0 {
				bc = b.Start()
			}
		case err := <-cc:
			log.Printf("error: %v", err)
			if retries > 0 {
				cc = c.Start()
			}
		case sig := <-sigc:
			log.Printf("got signal %v", sig)
			break retryLoop
		}
		log.Printf("(%v retries remaining)", retries)
	}
	log.Printf("shutting down")
	// Stop all services.
	a.Stop()
	b.Stop()
	c.Stop()
	// Wait for all services to finish.
	if err := <-ac; err != nil {
		log.Printf("a error: %v", err)
	}
	if err := <-bc; err != nil {
		log.Printf("b error: %v", err)
	}
	if err := <-cc; err != nil {
		log.Printf("c error: %v", err)
	}
}
*/
