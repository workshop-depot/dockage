package dockage

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
