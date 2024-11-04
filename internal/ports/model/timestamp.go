package model

import "time"

type Timestamp struct {
	LocalTime   time.Time `json:"local_time"`
	LogicalTime int       `json:"logical_time"`
	Pid         int       `json:"pid"`
}
