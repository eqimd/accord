package ports

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/eqimd/accord/internal/cluster"
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

	mux.Get("/pid", s.pid)
	mux.Get("/snapshot", s.snapshot)

	return mux
}

func (s *replicaServer) preAccept(w http.ResponseWriter, request *http.Request) {
	var preAcceptReq model.PreAcceptRequest

	err := json.NewDecoder(request.Body).Decode(&preAcceptReq)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	msgTs := preAcceptReq.TsProposed.ToMessageTimestamp()

	tsProp, deps, err := s.replica.PreAccept(
		preAcceptReq.Sender,
		preAcceptReq.Txn.ToMessageTxn(),
		preAcceptReq.TxnKeys,
		msgTs,
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	resp := &model.PreAcceptResponse{
		TsProposed: model.FromMessageTimestamp(tsProp),
		Deps:       model.ModelDepsFromMessage(deps),
	}

	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		// TODO log
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
}

func (s *replicaServer) accept(w http.ResponseWriter, request *http.Request) {
	var acceptReq model.AcceptRequest

	err := json.NewDecoder(request.Body).Decode(&acceptReq)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	ts0 := acceptReq.TsProposed.ToMessageTimestamp()

	txnDeps, err := s.replica.Accept(
		acceptReq.Sender,
		acceptReq.Txn.ToMessageTxn(),
		acceptReq.TxnKeys,
		ts0,
		acceptReq.TsExecution.ToMessageTimestamp(),
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	acceptResp := &model.AcceptResponse{
		Deps: model.ModelDepsFromMessage(txnDeps),
	}

	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(acceptResp)
	if err != nil {
		// TODO log
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
}

func (s *replicaServer) commit(w http.ResponseWriter, request *http.Request) {
	var commitReq model.CommitRequest

	if err := json.NewDecoder(request.Body).Decode(&commitReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

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
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *replicaServer) read(w http.ResponseWriter, request *http.Request) {
	var readReq model.ReadRequest

	if err := json.NewDecoder(request.Body).Decode(&readReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	reads, err := s.replica.Read(
		readReq.Sender,
		readReq.Txn.ToMessageTxn(),
		readReq.TxnKeys,
		readReq.TsExecution.ToMessageTimestamp(),
		model.MessageDepsFromModel(readReq.Deps),
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}

	readResp := &model.ReadResponse{
		Reads: reads,
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(readResp); err != nil {
		// TODO log
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
}

func (s *replicaServer) apply(w http.ResponseWriter, request *http.Request) {
	var applyReq model.ApplyRequest

	if err := json.NewDecoder(request.Body).Decode(&applyReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))

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
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}
}

func (s *replicaServer) pid(w http.ResponseWriter, request *http.Request) {
	resp := &model.PidResponse{
		Pid: os.Getpid(),
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))

		return
	}
}

func (s *replicaServer) snapshot(w http.ResponseWriter, request *http.Request) {
	// TODO
	snapshot, _ := s.replica.Snapshot()

	resp := &model.SnapshotResponse{
		Values: snapshot,
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
}
