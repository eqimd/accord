package environment

import (
	"fmt"
	"net/http"
	"time"

	"github.com/eqimd/accord/internal/common"
	"github.com/eqimd/accord/internal/message"
	"github.com/eqimd/accord/internal/ports/model"
	"github.com/go-resty/resty/v2"
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

	client := resty.New()
	client.BaseURL = e.replicaToAddr[to]
	client.SetTimeout(5 * time.Minute)

	var preAcceptResp model.PreAcceptResponse

	rr, err := client.R().SetBody(preAcceptReq).SetResult(&preAcceptResp).Post("/preaccept")
	if err != nil {
		return message.Timestamp{}, message.TxnDependencies{}, err
	}

	if rr.StatusCode() != http.StatusOK {
		return message.Timestamp{}, message.TxnDependencies{}, fmt.Errorf("error: %s", rr.Body())
	}

	return preAcceptResp.TsProposed.ToMessageTimestamp(), model.MessageDepsFromModel(preAcceptResp.Deps), nil
}

func (e *HTTPEnv) Accept(
	from, to int,
	txn message.Transaction,
	keys []string,
	ts0 message.Timestamp,
	ts message.Timestamp,
) (message.TxnDependencies, error) {
	acceptReq := &model.AcceptRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
		TsProposed:  model.FromMessageTimestamp(ts0),
		TxnKeys:     keys,
		TsExecution: model.FromMessageTimestamp(ts),
	}

	client := resty.New()
	client.BaseURL = e.replicaToAddr[to]
	client.SetTimeout(5 * time.Minute)

	var acceptResp model.AcceptResponse

	rr, err := client.R().SetBody(acceptReq).SetResult(&acceptResp).Post("/accept")
	if err != nil {
		return message.TxnDependencies{}, err
	}

	if rr.StatusCode() != http.StatusOK {
		return message.TxnDependencies{}, fmt.Errorf("error: %s", rr.Body())
	}

	return model.MessageDepsFromModel(acceptResp.Deps), nil
}

func (e *HTTPEnv) Commit(
	from, to int,
	txn message.Transaction,
	ts0 message.Timestamp,
	ts message.Timestamp,
	deps message.TxnDependencies,
) error {
	commitReq := &model.CommitRequest{
		Sender: from,
		Txn: model.Txn{
			Hash: txn.TxnHash,
			Ts:   model.FromMessageTimestamp(txn.Timestamp),
		},
		TsProposed:  model.FromMessageTimestamp(ts0),
		TsExecution: model.FromMessageTimestamp(ts),
		Deps:        model.ModelDepsFromMessage(deps),
	}

	client := resty.New()
	client.BaseURL = e.replicaToAddr[to]
	client.SetTimeout(5 * time.Minute)

	rr, err := client.R().SetBody(commitReq).Post("/commit")
	if err != nil {
		return err
	}

	if rr.StatusCode() != http.StatusOK {
		return fmt.Errorf("error: %s", rr.Body())
	}

	return nil
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

	client := resty.New()
	client.BaseURL = e.replicaToAddr[to]
	client.SetTimeout(5 * time.Minute)

	var readResp model.ReadResponse

	rr, err := client.R().SetBody(readReq).SetResult(&readResp).Post("/read")
	if err != nil {
		return nil, err
	}

	if rr.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("error: %s", rr.Body())
	}

	return readResp.Reads, nil
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

	client := resty.New()
	client.BaseURL = e.replicaToAddr[to]
	client.SetTimeout(5 * time.Minute)

	rr, err := client.R().SetBody(applyReq).Post("/apply")
	if err != nil {
		return err
	}

	if rr.StatusCode() != http.StatusOK {
		return fmt.Errorf("error: %s", rr.Body())
	}

	return nil
}

func (e *HTTPEnv) ReplicaPidsByShard(shardID int) common.Set[int] {
	return common.SetFromSlice(e.shardToReplicas[shardID])
}
