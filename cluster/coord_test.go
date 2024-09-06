package cluster

import (
	"testing"

	"github.com/eqimd/accord/common"
	"github.com/stretchr/testify/require"
)

func TestCommon(t *testing.T) {
	pids := common.Set[int]{}
	shardID := 0

	for i := 0; i < 2; i++ {
		pid := (shardID * 2) + i
		pids.Add(pid)
	}


	require.Len(t, pids, 1)
}
