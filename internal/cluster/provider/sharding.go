package provider

import "github.com/eqimd/accord/internal/common"

type Sharding interface {
	ShardToKeys(keys common.Set[string]) map[int]common.Set[string]
}
