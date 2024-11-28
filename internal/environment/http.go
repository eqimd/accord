package environment

import (
	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
	"github.com/eqimd/accord/internal/ports/model"
)

type HTTPEnv struct {
	replicaToAddr   map[int]string
	shardToReplicas map[int][]int
}

func NewHTTP(shardToReplicas map[int][]int, replicaToAddr map[int]string) *HTTPEnv {
	env := &HTTPEnv{
		replicaToAddr:   replicaToAddr,
		shardToReplicas: shardToReplicas,
	}

	return env
}

func (e *HTTPEnv) PreAccept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts0 message.Timestamp,
) (message.Timestamp, message.TxnDependencies, error) {
	preAcceptReq := &model.PreAcceptRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
		TsProposed: model.FromMessageTimestamp(ts0),
		TxnKeys:    keys,
	}

	var preAcceptResp model.PreAcceptResponse

	err := common.SendPost(
		e.replicaToAddr[to]+"/preaccept",
		preAcceptReq,
		&preAcceptResp,
	)

	return preAcceptResp.TsProposed.ToMessageTimestamp(), model.MessageDepsFromModel(preAcceptResp.Deps), err
}

func (e *HTTPEnv) Accept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	acceptReq := &model.AcceptRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
		TxnKeys:     keys,
		TsExecution: model.FromMessageTimestamp(ts),
	}

	var acceptResp model.AcceptResponse

	err := common.SendPost(
		e.replicaToAddr[to]+"/accept",
		acceptReq,
		&acceptResp,
	)

	return model.MessageDepsFromModel(acceptResp.Deps), err
}

func (e *HTTPEnv) Commit(
	from, to int,
	txn message.Transaction,
) error {
	commitReq := &model.CommitRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
	}

	err := common.SendPost(
		e.replicaToAddr[to]+"/commit",
		commitReq,
		nil,
	)

	return err
}

func (e *HTTPEnv) Read(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts message.Timestamp,
	deps message.TxnDependencies,
) (map[string]string, error) {
	readReq := &model.ReadRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
		TsExecution: model.FromMessageTimestamp(ts),
		TxnKeys:     keys,
		Deps:        model.ModelDepsFromMessage(deps),
	}

	var readResp model.ReadResponse

	err := common.SendPost(
		e.replicaToAddr[to]+"/read",
		readReq,
		&readResp,
	)

	return readResp.Reads, err
}

func (e *HTTPEnv) Apply(
	from, to int,
	txn message.Transaction,
	ts message.Timestamp,
	deps message.TxnDependencies,
	result map[string]string,
) error {
	applyReq := &model.ApplyRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
		TsExecution: model.FromMessageTimestamp(ts),
		Deps:        model.ModelDepsFromMessage(deps),
		Result:      result,
	}

	err := common.SendPost(
		e.replicaToAddr[to]+"/apply",
		applyReq,
		nil,
	)

	return err
}

func (e *HTTPEnv) ReplicaPidsByShard(shardID int) common.Set[int] {
	return common.SetFromSlice(e.shardToReplicas[shardID])
}
