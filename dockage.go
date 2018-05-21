package dockage

// GOCACHE=off

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dgraph-io/badger"
	"github.com/tidwall/gjson"
)

// sentinel errors
var (
	ErrInvalidJSONDoc = errors.New("invalid json doc")
	ErrNoID           = errors.New("no id in doc")
)

// Options are params for creating DB object.
type Options struct {
	// 1. Mandatory flags
	// -------------------
	// Directory to store the data in. Should exist and be writable.
	Dir string
	// Directory to store the value log in. Can be the same as Dir. Should
	// exist and be writable.
	ValueDir string
}

// DB .
type DB struct {
	db    *badger.DB
	views views
}

// Open .
func Open(opt Options) (_db *DB, _err error) {
	_opt := badger.DefaultOptions
	_opt.Dir = opt.Dir
	_opt.ValueDir = opt.ValueDir
	bdb, err := badger.Open(_opt)
	if err != nil {
		return nil, err
	}
	return &DB{db: bdb}, nil
}

// Close .
func (db *DB) Close() error { return db.db.Close() }

// AddView .
func (db *DB) AddView(v View) { db.views = append(db.views, v) }

// Query .
func (db *DB) Query(params Q) (_res []struct{ Key, Val []byte }, _err error) {
	params.init()
	start, end, prefix := params.stopWords()

	skip := params.Skip
	limit := params.Limit
	var (
		applySkip, applyLimit bool
	)
	if skip > 0 {
		applySkip = true
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 0 {
		applyLimit = true
	}

	body := func(itr interface{ Item() *badger.Item }) error {
		item := itr.Item()
		k := item.KeyCopy(nil)
		if params.View == "" { // TODO: add a separate space/bucket for KVs (?)
			if bytes.HasPrefix(k, []byte(sep)) {
				return nil
			}
		}
		skip--
		if applySkip && skip >= 0 {
			return nil
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if applyLimit && limit <= 0 {
			return nil
		}
		limit--
		if len(end) > 0 {
			if bytes.Compare(k, end) > 0 {
				return nil
			}
		}
		polishedKey := k
		if params.View != "" {
			pfx := pat(params.View+xtok) + sep
			polishedKey = bytes.TrimPrefix(polishedKey, []byte(pfx))
			polishedKey = bytes.Split(polishedKey, []byte(sep))[0]
		}
		_res = append(_res, kv{Key: polishedKey, Val: v})
		return nil
	}

	_err = db.db.View(func(txn *badger.Txn) error {
		var opt badger.IteratorOptions
		opt.PrefetchValues = true
		opt.PrefetchSize = limit
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(start); it.ValidForPrefix(prefix); it.Next() {
			if err := body(it); err != nil {
				return err
			}
		}
		return nil
	})

	return
}

// Delete .
func (db *DB) Delete(ids ...string) (_err error) {
	if len(ids) == 0 {
		return
	}
	_err = db.db.Update(func(txn *badger.Txn) error {
		for _, vid := range ids {
			if err := txn.Delete([]byte(vid)); err != nil {
				return err
			}
		}
		for _, vid := range ids {
			if err := db.views.buildAll(txn, []byte(vid), nil); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// Put .
func (db *DB) Put(docs ...interface{}) (_err error) {
	if len(docs) == 0 {
		return
	}
	_err = db.db.Update(func(txn *badger.Txn) error {
		var builds []kv
		for _, vdoc := range docs {
			js, id, err := prep(vdoc)
			if err != nil {
				return err
			}
			if err := txn.Set(id, js); err != nil {
				return err
			}
			builds = append(builds, kv{Key: id, Val: js})
		}
		for _, v := range builds {
			if err := db.views.buildAll(txn, v.Key, v.Val); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

func (db *DB) _all() (_res []kv, _err error) {
	_err = db.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		for itr.Rewind(); itr.Valid(); itr.Next() {
			itm := itr.Item()
			var kv kv
			kv.Key = itm.KeyCopy(nil)
			var err error
			kv.Val, err = itm.ValueCopy(nil)
			if err != nil {
				return err
			}
			_res = append(_res, kv)
		}
		return nil
	})
	return
}

func prep(doc interface{}) (_js, _id []byte, _err error) {
	switch x := doc.(type) {
	case string:
		_js = []byte(x)
	case []byte:
		_js = x
	default:
		js, err := json.Marshal(doc)
		if err != nil {
			_err = err
			return
		}
		_js = js
	}
	if !gjson.Valid(string(_js)) {
		_err = ErrInvalidJSONDoc
		return
	}
	resid := gjson.Get(string(_js), "id")
	if !resid.Exists() {
		_err = ErrNoID
		return
	}
	_id = []byte(resid.String())
	return
}

// kv tuple
type kv struct {
	Key, Val []byte
}

// Q query parameters
type Q struct {
	View               string
	Start, End, Prefix []byte
	Skip, Limit        int
}

func (q *Q) init() {
	if q.Limit <= 0 {
		q.Limit = 100
	}
}

func (q *Q) stopWords() (start, end, prefix []byte) {
	if q.View != "" {
		pfx := pat(q.View + xtok)
		start = []byte(pfx + pat(string(q.Start)))
		end = []byte(pfx + pat(string(q.End)))
		if len(q.Prefix) > 0 {
			prefix = []byte(pfx + pat(string(q.Prefix)))
		} else {
			prefix = start
		}
		return
	}
	start = append([]byte(q.View), q.Start...)
	end = append([]byte(q.View), q.End...)
	if len(q.Prefix) > 0 {
		prefix = append([]byte(q.View), q.Prefix...)
	} else {
		prefix = start
	}
	return
}
