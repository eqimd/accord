package discovery

import (
	"sync"

	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/ports/model"
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
			var pidResp model.PidResponse

			err := common.SendGet(addr+"/pid", &pidResp)
			if err != nil {
				return err
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
