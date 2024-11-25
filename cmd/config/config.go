package config

type Config struct {
	Replicas []*ReplicaConfig `json:"replicas"`
}

type ReplicaConfig struct {
	Address string `json:"address"`
	ShardID int    `json:"shard_id"`
}
