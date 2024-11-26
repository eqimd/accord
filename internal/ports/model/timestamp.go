package model

import (
	"github.com/eqimd/accord/internal/message"
)

type Timestamp struct {
	LocalTime   uint64 `json:"local_time"`
	LogicalTime int    `json:"logical_time"`
	Pid         int    `json:"pid"`
}

func (t *Timestamp) ToMessageTimestamp() message.Timestamp {
	return message.Timestamp{
		LocalTime:   t.LocalTime,
		LogicalTime: t.LogicalTime,
		Pid:         t.Pid,
	}
}

func FromMessageTimestamp(ts message.Timestamp) Timestamp {
	return Timestamp{
		LocalTime:   ts.LocalTime,
		LogicalTime: ts.LogicalTime,
		Pid:         ts.Pid,
	}
}
