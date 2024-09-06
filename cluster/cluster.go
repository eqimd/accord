package cluster

import "fmt"

type Cluster struct {
	env *Environment
}

func NewCluster(
	shardsCount int, replicasPerShard int,
) *Cluster {
	env := NewEnvironment(shardsCount, replicasPerShard)
	cluster := &Cluster{
		env: env,
	}

	return cluster
}

func (cl *Cluster) Exec(query string, coordinatorPid int) (string, error) {
	result, err := cl.env.coordinators[coordinatorPid].Exec(query)

	return result, err
}

func (cl *Cluster) Snapshot() map[string]string {
	kv := map[string]string{}

	for i := 0; i < cl.env.Shards; i++ {
		for j := 0; j < cl.env.ReplicasPerShard; j++ {
			replicaPid := (i * cl.env.ReplicasPerShard) + j
			snapshot := cl.env.replicas[replicaPid].storage.Snapshot()

			for k, v := range snapshot {
				kv[k] = v
			}

			fmt.Println(replicaPid, ":", snapshot)
		}
	}

	return kv
}
