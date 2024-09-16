package ports

import (
	"net/http"

	"github.com/eqimd/accord/cluster"
	"github.com/go-chi/chi/v5"
)

type replicaServer struct {
	replica *cluster.Replica
}

func NewReplicaHandler(replica *cluster.Replica) http.Handler {
	server := &replicaServer{
		replica: replica,
	}

	return server.newHandler()
}

func (s *replicaServer) newHandler() http.Handler {
	mux := chi.NewMux()

	mux.Post("/preaccept", nil)
	mux.Post("/accept", s.accept)
	mux.Post("/commit", nil)
	mux.Post("/read", nil)
	mux.Post("/apply", nil)

	return mux
}

func (s *replicaServer) accept(w http.ResponseWriter, request *http.Request) {
}
