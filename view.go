package dockage

import (
	"strings"

	"github.com/dgraph-io/badger"
)

type views []View

func (vl views) buildAll(txn *badger.Txn, k, v []byte) error {
	for _, ix := range vl {
		if err := ix.build(txn, k, v); err != nil {
			return err
		}
	}
	return nil
}

// View .
type View struct {
	name  string
	mapfn func(k, v []byte) []KV
}

// NewView .
func NewView(name string,
	mapfn func(k, v []byte) []KV) View {
	return View{
		name:  name,
		mapfn: mapfn,
	}
}

func (vv View) build(txn *badger.Txn, k, v []byte) error {
	_k2x := pat(vv.name + ktox)
	_x2k := pat(vv.name + xtok)

	_markedKey := pat(string(k))
	_k := _k2x + _markedKey

	// TODO: delete while prefix exists instead of this fixed size fetch.
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	// delete previously calculated index for this key
	it := txn.NewIterator(opt)
	defer it.Close()
	prefix := []byte(_k)
	var toDelete [][]byte
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return err
		}
		toDelete = append(toDelete, k)
		toDelete = append(toDelete, v)
	}
	for _, v := range toDelete {
		if err := txn.Delete(v); err != nil {
			if err != badger.ErrEmptyKey {
				return err
			}
		}
	}

	if v == nil {
		return nil
	}

	emitted := vv.mapfn(k, v)
	for _, kv := range emitted {
		wix := pat(string(kv.Key))
		k2x := _k + wix
		x2k := _x2k + wix + _markedKey
		if err := txn.Set([]byte(k2x), []byte(x2k)); err != nil {
			return err
		}
		if err := txn.Set([]byte(x2k), kv.Val); err != nil {
			return err
		}
	}

	return nil
}

const (
	sep  = "^" // ^BUCKET^PART^PART^PART
	ktox = ">"
	xtok = "<"
)

func pat(s ...string) string {
	return sep + strings.Join(s, sep)
}
