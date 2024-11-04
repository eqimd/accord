package message

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimestamp(t *testing.T) {
	ts1 := Timestamp{
		LocalTime:   time.Now(),
		LogicalTime: 1,
		Pid:         3,
	}

	ts2 := Timestamp{
		LocalTime:   ts1.LocalTime,
		LogicalTime: 0,
		Pid:         4,
	}

	require.False(t, ts1.Less(ts2))
}
