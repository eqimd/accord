package sharding

import (
	"hash/fnv"

	"github.com/eqimd/accord/internal/common"
)

type Hash struct {
	// index of the slice is virtual shard id; value is real shard id
	virtualShards []int
}

func NewHash(shards common.Set[int]) *Hash {
	hash := &Hash{
		virtualShards: make([]int, len(shards)),
	}

	vId := 0
	for shardID := range shards {
		hash.virtualShards[vId] = shardID

		vId++
	}

	return hash
}

// returns mapping of (shard id) -> (set of keys for this shard)
func (h *Hash) ShardToKeys(keys []string) map[int][]string {
	shardToKeys := map[int][]string{}

	for _, key := range keys {
		virtID := h.getVirtualShardByKey(key)

		shardID := h.virtualShards[virtID]

		shardToKeys[shardID] = append(shardToKeys[shardID], key)
	}

	return shardToKeys
}

func (h *Hash) getVirtualShardByKey(key string) int {
	fnv := fnv.New32a()
	fnv.Write([]byte(key))

	return int(fnv.Sum32() % uint32(len(h.virtualShards)))
}
