package ports

import (
	"encoding/json"
	"net/http"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/ports/model"
	"github.com/go-chi/chi/v5"
)

type coordinatorServer struct {
	coordinator *cluster.Coordinator
}

func NewCoordinatorHandler(coordinator *cluster.Coordinator) http.Handler {
	server := &coordinatorServer{
		coordinator: coordinator,
	}

	return server.newHandler()
}

func (s *coordinatorServer) newHandler() http.Handler {
	mux := chi.NewMux()

	mux.Post("/execute", s.execute)

	return mux
}

func (s *coordinatorServer) execute(w http.ResponseWriter, request *http.Request) {
	var executeReq model.ExecuteRequest

	if err := json.NewDecoder(request.Body).Decode(&executeReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	resp, err := s.coordinator.Exec(executeReq.Query)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	executeResp := &model.ExecuteResponse{
		Response: resp,
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(executeResp); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

		return
	}
}
