package dockage

import "github.com/dgraph-io/badger"

//-----------------------------------------------------------------------------

type transaction struct {
	tx *badger.Txn
}

func newTransaction(tx *badger.Txn) *transaction {
	return &transaction{tx: tx}
}

//-----------------------------------------------------------------------------
