package cluster

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

func (cl *Cluster) Snapshot() map[int]map[string]string {
	replicaToSnapshot := map[int]map[string]string{}

	for _, replica := range cl.env.replicas {
		replicaToSnapshot[replica.pid] = replica.storage.Snapshot()
	}

	return replicaToSnapshot
}
