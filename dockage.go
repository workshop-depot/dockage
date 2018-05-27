// Package dockage is an embedded document (json) database.
package dockage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/dgraph-io/badger"
	"github.com/tidwall/sjson"
)

//-----------------------------------------------------------------------------

// DB represents a database instance.
type DB struct {
	db     *badger.DB
	views  views
	sqView View
	sq     *badger.Sequence
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
	sq, err := bdb.GetSequence([]byte(pat4Sys(dbseq)), 512)
	if err != nil {
		reserr = err
		return
	}
	resdb = &DB{db: bdb, sq: sq}
	resdb.sqView = newView(viewdbseq,
		func(em Emitter, k, v []byte) (inf interface{}, err error) {
			sq, err := resdb.sq.Next()
			if err != nil {
				return nil, err
			}
			ix := make([]byte, 8)
			binary.BigEndian.PutUint64(ix, sq)
			ix = []byte(hex.EncodeToString(ix))
			em.Emit(ix, nil)
			return ix, nil
		})
	return
}

// Close closes the database.
func (db *DB) Close() error {
	db.sq.Release()
	return db.db.Close()
}

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

			js, id, rev, err := prepdoc(vdoc)
			if err != nil {
				return err
			}

			qres, _, qerr := db.queryView(Q{View: viewdbseq, Start: id, Prefix: id}, txn, true)
			if qerr != nil {
				return qerr
			}
			if len(rev) == 0 {
				if len(qres) > 0 {
					if bytes.Compare(qres[0].Key, rev) != 0 {
						return ErrNoMatchRev
					}
				}
			} else if bytes.Compare(qres[0].Key, rev) != 0 {
				return ErrNoMatchRev
			}
			em := newViewEmitter(newTransaction(txn), db.sqView)
			resinf, reserr := em.build(id, js)
			if reserr != nil {
				return reserr
			}
			rev = resinf.([]byte)
			strjs, err := sjson.Set(string(js), "rev", string(rev))
			if err != nil {
				return err
			}
			js = []byte(strjs)

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
		var viewList views = append([]View{db.sqView}, db.views...)
		for _, vid := range ids {
			if err := txn.Delete([]byte(keysp + vid)); err != nil {
				return err
			}
		}
		for _, vid := range ids {
			tx := newTransaction(txn)
			if _, err := viewList.buildAll(tx, []byte(vid), nil); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// Query queries a view using provided parameters. If no View is provided, it searches
// all ids using parameters. Number of results is always limited - default 100 documents.
// If total count for a query is needed by setting params.Count to true, no documents
// will be returned - because it might be a costly action.
func (db *DB) Query(params Q) (reslist []Res, rescount int, reserr error) {
	reslist, rescount, reserr = db.queryView(params, nil)
	return
}

func (db *DB) queryView(params Q, parentTxn *badger.Txn, forIndexedKeys ...bool) (reslist []Res, rescount int, reserr error) {
	params.init()

	start, end, prefix := stopWords(params, forIndexedKeys...)

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

	qfn := func(txn *badger.Txn) error {
		var opt badger.IteratorOptions
		opt.PrefetchValues = true
		opt.PrefetchSize = limit
		return itrFunc(txn, opt, start, prefix, body)
	}
	if parentTxn == nil {
		reserr = db.db.View(qfn)
	} else {
		reserr = qfn(parentTxn)
	}
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
