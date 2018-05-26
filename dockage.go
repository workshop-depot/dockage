package dockage

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger"
)

//-----------------------------------------------------------------------------

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

// DeleteView deletes the data of a view.
func (db *DB) DeleteView(v string) (_err error) {
	name := string(_hash([]byte(v)))
	prefix := []byte(_pat4View(name))
	_err = db.db.Update(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()
		var todelete [][]byte
		for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
			item := itr.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(k) != 0 {
				todelete = append(todelete, k)
			}
			if len(v) != 0 {
				todelete = append(todelete, v)
			}
		}
		for _, vd := range todelete {
			if err := txn.Delete(vd); err != nil {
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
		var builds []KV
		for _, vdoc := range docs {
			js, id, err := _prep(vdoc)
			if err != nil {
				return err
			}
			if err := txn.Set(append([]byte(keysp), id...), js); err != nil {
				return err
			}
			builds = append(builds, KV{Key: id, Val: js})
		}
		for _, v := range builds {
			tx := newTransaction(txn)
			if _, err := db.views.buildAll(tx, v.Key, v.Val); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// Get .
func (db *DB) Get(ids ...string) (_res []KV, _err error) {
	if len(ids) == 0 {
		return
	}
	_err = db.db.View(func(txn *badger.Txn) error {
		for _, vid := range ids {
			vid := _pat4Key(vid)
			item, err := txn.Get([]byte(vid))
			if err != nil {
				return err
			}
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			k = bytes.TrimPrefix(k, []byte(keysp))
			_res = append(_res, KV{Key: k, Val: v})
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
			if err := txn.Delete([]byte(keysp + vid)); err != nil {
				return err
			}
		}
		for _, vid := range ids {
			tx := newTransaction(txn)
			if _, err := db.views.buildAll(tx, []byte(vid), nil); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// Query .
func (db *DB) Query(params Q) (_res []Res, _count int, _err error) {
	params.init()

	start, end, prefix := stopWords(params)

	skip, limit, applySkip, applyLimit := limits(params)

	body := func(itr interface{ Item() *badger.Item }) error {
		if params.Count {
			_count++
			skip--
			if applySkip && skip >= 0 {
				return nil
			}
			if applyLimit && limit <= 0 {
				return nil
			}
			limit--
			if len(end) > 0 {
				item := itr.Item()
				k := item.Key()
				if bytes.Compare(k, end) > 0 {
					return nil
				}
			}
			return nil
		}
		item := itr.Item()
		k := item.KeyCopy(nil)
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
		var index []byte
		polishedKey := k
		sppfx := []byte(keysp)
		if bytes.HasPrefix(polishedKey, sppfx) {
			polishedKey = bytes.TrimPrefix(polishedKey, sppfx)
		}
		sppfx = []byte(viewsp)
		if bytes.HasPrefix(polishedKey, sppfx) {
			parts := bytes.Split(polishedKey, sppfx)
			index = parts[2]
			polishedKey = parts[3]
		}
		var rs Res
		rs.Key = polishedKey
		rs.Val = v
		rs.Index = index
		_res = append(_res, rs)
		return nil
	}

	_err = db.db.View(func(txn *badger.Txn) error {
		var opt badger.IteratorOptions
		opt.PrefetchValues = true
		opt.PrefetchSize = limit
		return itrFunc(txn, opt, start, prefix, body)
	})

	if _count == 0 {
		_count = len(_res)
	}

	return
}

func limits(params Q) (skip, limit int, applySkip, applyLimit bool) {
	skip = params.Skip
	limit = params.Limit
	var ()
	if skip > 0 {
		applySkip = true
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 0 {
		applyLimit = true
	}
	return
}

func stopWords(params Q) (start, end, prefix []byte) {
	if params.View == "" {
		start = []byte(_pat4Key(string(params.Start)))
		if len(params.End) > 0 {
			end = []byte(_pat4Key(string(params.End)))
		}
		if len(params.Prefix) > 0 {
			prefix = []byte(_pat4Key(string(params.Prefix)))
		} else {
			prefix = start
		}
	} else {
		name := string(_hash([]byte(params.View)))
		pfx := _pat4View(name + viewx2k)
		start = []byte(pfx + _pat4View(string(params.Start)))
		if len(params.End) > 0 {
			end = []byte(pfx + _pat4View(string(params.End)))
		}
		if len(params.Prefix) > 0 {
			prefix = []byte(pfx + _pat4View(string(params.Prefix)))
		} else {
			prefix = []byte(pfx)
		}
	}
	return
}

func itrFunc(txn *badger.Txn,
	opt badger.IteratorOptions,
	start, prefix []byte,
	bodyFunc func(itr interface{ Item() *badger.Item }) error) error {
	itr := txn.NewIterator(opt)
	defer itr.Close()
	for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
		if err := bodyFunc(itr); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) _all() (_res []KV, _err error) {
	_err = db.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			itm := itr.Item()
			var kv KV
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

//-----------------------------------------------------------------------------

// Q query parameters
type Q struct {
	View               string
	Start, End, Prefix []byte
	Skip, Limit        int
	Count              bool
}

func (q *Q) init() {
	if q.Limit <= 0 {
		q.Limit = 100
	}
}

//-----------------------------------------------------------------------------

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

//-----------------------------------------------------------------------------

// sentinel errors
var (
	ErrInvalidJSONDoc = errors.New("invalid json doc")
	ErrNoID           = errors.New("no id in doc")
	ErrInvalidID      = fmt.Errorf("id must not contain these characters: %s %s %s %s %s",
		viewsp,
		keysp,
		syssp,
		viewk2x,
		viewx2k)
)

//-----------------------------------------------------------------------------
