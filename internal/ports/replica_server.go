package ports

import (
	"encoding/json"
	"net/http"

	"github.com/eqimd/accord/internal/cluster"
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/ports/model"
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

	msgTs := preAcceptReq.TsProposed.ToMessageTimestamp()

	tsProp, deps, err := s.replica.PreAccept(
		preAcceptReq.Sender,
		preAcceptReq.Txn.ToMessageTxn(),
		common.SetFromSlice(preAcceptReq.TxnKeys),
		msgTs,
	)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	resp := &model.PreAcceptResponse{
		TsProposed: model.FromMessageTimestamp(tsProp),
		Deps:       model.ModelDepsFromMessage(deps),
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		// TODO log
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *replicaServer) accept(w http.ResponseWriter, request *http.Request) {
	var acceptReq model.AcceptRequest

	err := json.NewDecoder(request.Body).Decode(&acceptReq)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	ts0 := acceptReq.TsProposed.ToMessageTimestamp()

	txnDeps, err := s.replica.Accept(
		acceptReq.Sender,
		acceptReq.Txn.ToMessageTxn(),
		common.SetFromSlice(acceptReq.TxnKeys),
		ts0,
		acceptReq.TsExecution.ToMessageTimestamp(),
	)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	acceptResp := &model.AcceptResponse{
		Deps: model.ModelDepsFromMessage(txnDeps),
	}

	err = json.NewEncoder(w).Encode(acceptResp)
	if err != nil {
		// TODO log
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *replicaServer) commit(w http.ResponseWriter, request *http.Request) {
	var commitReq model.CommitRequest

	if err := json.NewDecoder(request.Body).Decode(&commitReq); err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	ts0 := commitReq.TsProposed.ToMessageTimestamp()

	err := s.replica.Commit(
		commitReq.Sender,
		commitReq.Txn.ToMessageTxn(),
		ts0,
		commitReq.TsExecution.ToMessageTimestamp(),
		model.MessageDepsFromModel(commitReq.Deps),
	)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *replicaServer) read(w http.ResponseWriter, request *http.Request) {
	var readReq model.ReadRequest

	if err := json.NewDecoder(request.Body).Decode(&readReq); err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	reads, err := s.replica.Read(
		readReq.Sender,
		readReq.Txn.ToMessageTxn(),
		common.SetFromSlice(readReq.TxnKeys),
		readReq.TsExecution.ToMessageTimestamp(),
		model.MessageDepsFromModel(readReq.Deps),
	)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	readResp := &model.ReadResponse{
		Reads: reads,
	}

	if err := json.NewEncoder(w).Encode(readResp); err != nil {
		// TODO log
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *replicaServer) apply(w http.ResponseWriter, request *http.Request) {
	var applyReq model.ApplyRequest

	if err := json.NewDecoder(request.Body).Decode(&applyReq); err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	err := s.replica.Apply(
		applyReq.Sender,
		applyReq.Txn.ToMessageTxn(),
		applyReq.TsExecution.ToMessageTimestamp(),
		model.MessageDepsFromModel(applyReq.Deps),
		applyReq.Result,
	)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
}
