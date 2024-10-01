package ports

import (
	"encoding/json"
	"net/http"

	"github.com/eqimd/accord/cluster"
	"github.com/eqimd/accord/common"
	"github.com/eqimd/accord/message"
	"github.com/eqimd/accord/ports/model"
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

	mux.Post("/preaccept", s.preAccept)
	mux.Post("/accept", s.accept)
	mux.Post("/commit", s.commit)
	mux.Post("/read", s.read)
	mux.Post("/apply", s.apply)

	return mux
}

func (s *replicaServer) preAccept(w http.ResponseWriter, request *http.Request) {
	var preAcceptReq model.PreAcceptRequest

	err := json.NewDecoder(request.Body).Decode(&preAcceptReq)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	ts0 := message.Timestamp{
		LocalTime:   preAcceptReq.TsProposed.LocalTime,
		LogicalTime: preAcceptReq.TsProposed.LogicalTime,
		Pid:         preAcceptReq.TsProposed.Pid,
	}

	tsProp, deps, err := s.replica.PreAccept(
		preAcceptReq.Sender,
		message.Transaction{
			TxnHash:   preAcceptReq.TxnHash,
			Timestamp: ts0,
		},
		common.SetFromSlice(preAcceptReq.TxnKeys),
		ts0,
	)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	modelDeps := make([]model.Txn, 0, len(deps.Deps))
	for d := range deps.Deps {
		md := model.Txn{
			Hash: d.TxnHash,
			Ts: model.Timestamp{
				LocalTime:   d.Timestamp.LocalTime,
				LogicalTime: d.Timestamp.LogicalTime,
				Pid:         d.Timestamp.Pid,
			},
		}
		modelDeps = append(modelDeps, md)
	}

	resp := &model.PreAcceptResponse{
		TsProposed: model.Timestamp{
			LocalTime:   tsProp.LocalTime,
			LogicalTime: tsProp.LogicalTime,
			Pid:         tsProp.Pid,
		},
		Deps: modelDeps,
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		// TODO log
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *replicaServer) accept(w http.ResponseWriter, request *http.Request) {

}

func (s *replicaServer) commit(w http.ResponseWriter, request *http.Request) {

}

func (s *replicaServer) read(w http.ResponseWriter, request *http.Request) {

}

func (s *replicaServer) apply(w http.ResponseWriter, request *http.Request) {

}
