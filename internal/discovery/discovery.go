package discovery

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/eqimd/accord/internal/ports/model"
	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
)

type DiscoveryPids struct {
	AddrToPid map[string]int
}

func DiscoverReplicas(addrs []string) (*DiscoveryPids, error) {
	pids := &DiscoveryPids{
		AddrToPid: make(map[string]int),
	}

	var errGroup errgroup.Group
	var mu sync.Mutex

	for _, addr := range addrs {
		addr := addr

		errGroup.Go(func() error {
			client := resty.New()
			client.BaseURL = addr

			var pidResp model.PidResponse

			rr, err := client.R().SetResult(&pidResp).Get("/pid")
			if err != nil {
				return err
			}

			if rr.StatusCode() != http.StatusOK {
				return fmt.Errorf("error: %s", rr.Body())
			}

			mu.Lock()

			pids.AddrToPid[addr] = pidResp.Pid

			mu.Unlock()

			return nil
		})
	}

	err := errGroup.Wait()

	return pids, err
}
