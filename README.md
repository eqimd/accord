## Accord Consensus Implementation

The repository contains simple in-memory key-value distributed database implementation of Accord Consensus protocol in Golang.

### Coordinator and Replica

Realization of coordinator algorithm can be found under `internal/coordinator` folder.

Realization of replica algorithm can be found under `internal/replica` folder.

### Sharding

Repo also contains simple sharding at `internal/sharding`. If you want to add another sharding algorithm, just create realization with needed methods, and pass it to coordinator.

### gRPC protobuf

Proto file to reach nodes can be found at `proto/` folder.

