package proto

func TsEqual(ts1 *TxnTimestamp, ts2 *TxnTimestamp) bool {
	return *ts1.LocalTime == *ts2.LocalTime && *ts1.LogicalTime == *ts2.LogicalTime && *ts1.Pid == *ts2.Pid
}

func TsLess(ts1 *TxnTimestamp, ts2 *TxnTimestamp) bool {
	if *ts1.LocalTime < *ts2.LocalTime {
		return true
	}

	if *ts1.LocalTime > *ts2.LocalTime {
		return false
	}

	if *ts1.LogicalTime < *ts2.LogicalTime {
		return true
	}

	if *ts1.LogicalTime > *ts2.LogicalTime {
		return false
	}

	if *ts1.Pid < *ts2.Pid {
		return true
	}

	if *ts1.Pid > *ts2.Pid {
		return false
	}

	return false
}
