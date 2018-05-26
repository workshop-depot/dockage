package dockage

// Res .
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
)
