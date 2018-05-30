package dockage

import "github.com/dgraph-io/badger"

//-----------------------------------------------------------------------------

type idd struct {
	ID  string
	Doc interface{}
}

// ViewFn function that emits view keys (and json docs as view values).
type ViewFn func(emitter Emitter, id string, doc interface{})

// View is a calculated, persistent index.
type View struct {
	name   string
	viewFn func(emitter Emitter, id string, doc interface{}) (inf interface{}, err error)
	hash   string
}

// NewView creates a new View. Function viewFn must have no side effects.
func NewView(name string, viewFn ViewFn) (resview View) {
	if name == "" {
		panic("name must be provided")
	}
	if viewFn == nil {
		panic("viewFn must be provided")
	}
	wrpvFn := func(emitter Emitter, id string, doc interface{}) (inf interface{}, err error) {
		viewFn(emitter, id, doc)
		return
	}
	resview = newView(name, wrpvFn)
	return
}

func newView(
	name string,
	viewFn func(emitter Emitter, id string, doc interface{}) (inf interface{}, err error)) (resview View) {
	resview = View{
		name:   name,
		viewFn: viewFn,
	}
	resview.hash = string(fnvhash([]byte(resview.name)))
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

func newViewEmitter(tx *transaction, v View) *viewEmitter {
	return &viewEmitter{txn: tx, v: v}
}

func (em *viewEmitter) Emit(viewKey, viewValue []byte) {
	em.emitted = append(em.emitted, KV{Key: viewKey, Val: viewValue})
}

func (em *viewEmitter) build(id string, doc interface{}) (resinf interface{}, reserr error) {
	partk2x := pat4View(em.v.hash + viewk2x)
	partx2k := pat4View(em.v.hash + viewx2k)

	markedKey := pat4View(id)
	preppedk := partk2x + markedKey

	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false

	// delete previously calculated index for this key
	txn := em.txn.tx
	itr := txn.NewIterator(opt)
	defer itr.Close()
	prefix := []byte(preppedk)
	var toDelete [][]byte
	for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
		item := itr.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			reserr = err
			return
		}
		toDelete = append(toDelete, k)
		toDelete = append(toDelete, v)
	}
	for _, v := range toDelete {
		if err := txn.Delete(v); err != nil {
			if err != badger.ErrEmptyKey {
				reserr = err
				return
			}
		}
	}

	if doc == nil {
		return
	}

	resinf, reserr = em.v.viewFn(em, id, doc)
	if reserr != nil {
		return
	}

	for _, kv := range em.emitted {
		wix := pat4View(string(kv.Key))
		k2x := preppedk + wix
		x2k := partx2k + wix + markedKey
		if reserr = txn.Set([]byte(k2x), []byte(x2k)); reserr != nil {
			return
		}
		if reserr = txn.Set([]byte(x2k), kv.Val); reserr != nil {
			return
		}
	}

	return
}

//-----------------------------------------------------------------------------

type views []View

func (vl views) buildAll(tx *transaction, id string, doc interface{}) (resinf interface{}, reserr error) {
	for _, ix := range vl {
		em := newViewEmitter(tx, ix)
		resinf, reserr = em.build(id, doc)
		if reserr != nil {
			return
		}
	}
	return
}

//-----------------------------------------------------------------------------
