package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReads(t *testing.T) {
	reads := map[string]string{
		"a": "b",
	}

	orig := &ReadResponse{
		Reads: reads,
	}

	b, err := json.Marshal(orig)
	require.NoError(t, err)

	nov := &ReadResponse{}

	err = json.Unmarshal(b, nov)
	require.NoError(t, err)

	require.Equal(t, reads, nov.Reads)
}
