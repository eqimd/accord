package message

import "time"

type Timestamp struct {
	Epoch       int
	LocalTime   time.Time
	LogicalTime int
	Pid         int
}

func (ts Timestamp) Less(other Timestamp) bool {
	if ts.Epoch < other.Epoch {
		return true
	}

	if other.Epoch < ts.Epoch {
		return false
	}

	if ts.LocalTime.Before(other.LocalTime) {
		return true
	}

	if other.LocalTime.Before(ts.LocalTime) {
		return false
	}

	if ts.LogicalTime < other.LogicalTime {
		return true
	}

	if other.LogicalTime < ts.LogicalTime {
		return false
	}

	if ts.Pid < other.Pid {
		return true
	}

	return false
}

func (ts Timestamp) Equal(other Timestamp) bool {
	return ts.Pid == other.Pid && ts.LogicalTime == other.LogicalTime && ts.LocalTime.Equal(other.LocalTime) && ts.Epoch == other.Epoch
}
