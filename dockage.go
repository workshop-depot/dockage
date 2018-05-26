// Package dockage is an embedded document (json) database.
package dockage

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger"
)

//-----------------------------------------------------------------------------

// DB represents a database instance.
type DB struct {
	db    *badger.DB
	views views
}

// Open opens the database with provided options.
func Open(opt Options) (resdb *DB, reserr error) {
	bopt := badger.DefaultOptions
	bopt.Dir = opt.Dir
	bopt.ValueDir = opt.ValueDir
	bdb, err := badger.Open(bopt)
	if err != nil {
		return nil, err
	}
	return &DB{db: bdb}, nil
}

// Close closes the database.
func (db *DB) Close() error { return db.db.Close() }

// AddView adds a view. All views must be added right after Open(...). It
// is not safe to call this method concurrently.
func (db *DB) AddView(v View) { db.views = append(db.views, v) }

// DeleteView deletes the data of a view.
func (db *DB) DeleteView(v string) (reserr error) {
	name := string(fnvhash([]byte(v)))
	prefix := []byte(pat4View(name))
	reserr = db.db.Update(func(txn *badger.Txn) error {
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

// Put a list of documents inside database, in a single transaction.
// Document must have a json field named "id".
func (db *DB) Put(docs ...interface{}) (reserr error) {
	if len(docs) == 0 {
		return
	}
	reserr = db.db.Update(func(txn *badger.Txn) error {
		var builds []KV
		for _, vdoc := range docs {
			js, id, err := prepdoc(vdoc)
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

// Get a list of documents based on their ids.
func (db *DB) Get(ids ...string) (reslist []KV, reserr error) {
	if len(ids) == 0 {
		return
	}
	reserr = db.db.View(func(txn *badger.Txn) error {
		for _, vid := range ids {
			vid := pat4Key(vid)
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
			reslist = append(reslist, KV{Key: k, Val: v})
		}
		return nil
	})
	return
}

// Delete a list of documents based on their ids.
func (db *DB) Delete(ids ...string) (reserr error) {
	if len(ids) == 0 {
		return
	}
	reserr = db.db.Update(func(txn *badger.Txn) error {
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

// Query queries a view using provided parameters. If no View is provided, it searches
// all ids using parameters. Number of results is always limited - default 100 documents.
// If total count for a query is needed, no documents will be returned.
func (db *DB) Query(params Q) (reslist []Res, rescount int, reserr error) {
	params.init()

	start, end, prefix := stopWords(params)

	skip, limit, applySkip, applyLimit := getlimits(params)

	body := func(itr interface{ Item() *badger.Item }) error {
		if params.Count {
			rescount++
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
		reslist = append(reslist, rs)
		return nil
	}

	reserr = db.db.View(func(txn *badger.Txn) error {
		var opt badger.IteratorOptions
		opt.PrefetchValues = true
		opt.PrefetchSize = limit
		return itrFunc(txn, opt, start, prefix, body)
	})

	if rescount == 0 {
		rescount = len(reslist)
	}

	return
}

func (db *DB) unboundAll() (reslist []KV, reserr error) {
	reserr = db.db.View(func(txn *badger.Txn) error {
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
			reslist = append(reslist, kv)
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
