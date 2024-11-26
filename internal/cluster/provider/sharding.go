package provider

type Sharding interface {
	ShardToKeys(keys []string) map[int][]string
}
