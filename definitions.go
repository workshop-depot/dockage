package dockage

import (
	"errors"
	"fmt"
)

// Res represents the result of a Query(...) call.
// Key is the document key, Index is the calculated index and Val is
// the calculated value by the view.
type Res struct {
	KV
	Index []byte
}

// KV tuple.
type KV struct {
	Key, Val []byte
}

// errors
var (
	ErrNoID      = errors.New("no id field in doc json")
	ErrNoRev     = errors.New("no rev field in doc json")
	ErrInvalidID = fmt.Errorf("id must not contain these characters: %s %s %s %s %s",
		viewsp,
		keysp,
		syssp,
		viewk2x,
		viewx2k)
	ErrNoMatchRev = errors.New("rev field in doc json not matching")
)

const (
	viewsp = "^" // view space - ^BUCKET^PART^PART^PART
	keysp  = "&" // key space
	syssp  = "."

	viewk2x = ">"
	viewx2k = "<"

	specials = viewsp +
		keysp +
		syssp +
		viewk2x +
		viewx2k

	dbseq     = "db_timestamp"
	viewdbseq = "view_db_timestamp"
)
