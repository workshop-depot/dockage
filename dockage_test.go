package dockage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQ(t *testing.T) {
	require := require.New(t)

	var q Q
	q.init()
	require.Equal(100, q.Limit)
}

func TestPat(t *testing.T) {
	require := require.New(t)

	require.Equal(sep, pat())
	require.Equal(sep+"KEY", pat("KEY"))
	require.Equal(sep+"KEY"+sep+"PART", pat("KEY", "PART"))
}
