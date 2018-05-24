package dockage

import "github.com/dgraph-io/badger"

//-----------------------------------------------------------------------------

// ViewFn function that generates view keys (and values).
type ViewFn func(emitter Emitter, k, v []byte)

// View is a calculated, persistent index.
type View struct {
	name   string
	viewFn func(emitter Emitter, k, v []byte) (inf interface{}, err error)
	hash   string
}

// NewView creates a new View. Function viewFn must have no side effects.
func NewView(name string, viewFn ViewFn) (_res View) {
	if name == "" {
		panic("name must be provided")
	}
	if viewFn == nil {
		panic("viewFn must be provided")
	}
	_viewFn := func(emitter Emitter, k, v []byte) (inf interface{}, err error) {
		viewFn(emitter, k, v)
		return
	}
	_res = newView(name, _viewFn)
	return
}

func newView(
	name string,
	viewFn func(emitter Emitter, k, v []byte) (inf interface{}, err error)) (_res View) {
	_res = View{
		name:   name,
		viewFn: viewFn,
	}
	_res.hash = string(_hash([]byte(_res.name)))
	return
}

// Emitter .
type Emitter interface {
	Emit(viewKey, viewValue []byte)
}

//-----------------------------------------------------------------------------

type viewEmitter struct {
	txn     *transaction
	v       View
	emitted []KV
}

func newViewEmitter(_tx *transaction, v View) *viewEmitter {
	return &viewEmitter{txn: _tx, v: v}
}

func (em *viewEmitter) Emit(viewKey, viewValue []byte) {
	em.emitted = append(em.emitted, KV{Key: viewKey, Val: viewValue})
}

func (em *viewEmitter) build(k, v []byte) (_inf interface{}, _err error) {
	_k2x := _pat4View(em.v.hash + viewk2x)
	_x2k := _pat4View(em.v.hash + viewx2k)

	_markedKey := _pat4View(string(k))
	_k := _k2x + _markedKey

	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false

	// delete previously calculated index for this key
	txn := em.txn.tx
	itr := txn.NewIterator(opt)
	defer itr.Close()
	prefix := []byte(_k)
	var toDelete [][]byte
	for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
		item := itr.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			_err = err
			return
		}
		toDelete = append(toDelete, k)
		toDelete = append(toDelete, v)
	}
	for _, v := range toDelete {
		if err := txn.Delete(v); err != nil {
			if err != badger.ErrEmptyKey {
				_err = err
				return
			}
		}
	}

	if v == nil {
		return
	}

	_inf, _err = em.v.viewFn(em, k, v)
	if _err != nil {
		return
	}

	for _, kv := range em.emitted {
		wix := _pat4View(string(kv.Key))
		k2x := _k + wix
		x2k := _x2k + wix + _markedKey
		if _err = txn.Set([]byte(k2x), []byte(x2k)); _err != nil {
			return
		}
		if _err = txn.Set([]byte(x2k), kv.Val); _err != nil {
			return
		}
	}

	return
}

//-----------------------------------------------------------------------------

type views []View

func (vl views) buildAll(tx *transaction, k, v []byte) (_inf interface{}, _err error) {
	for _, ix := range vl {
		em := newViewEmitter(tx, ix)
		_inf, _err = em.build(k, v)
		if _err != nil {
			return
		}
	}
	return
}

//-----------------------------------------------------------------------------
