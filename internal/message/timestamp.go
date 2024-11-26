package message

type Timestamp struct {
	LocalTime   uint64
	LogicalTime int
	Pid         int
}

func (ts Timestamp) Less(other Timestamp) bool {
	if ts.LocalTime < other.LocalTime {
		return true
	}

	if other.LocalTime < ts.LocalTime {
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
	return ts.Pid == other.Pid && ts.LogicalTime == other.LogicalTime && ts.LocalTime == other.LocalTime
}
