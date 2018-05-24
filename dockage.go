package dockage

import (
	"bytes"
	"errors"

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
func (db *DB) Query(params Q) (_res []Res, _err error) {
	params.init()

	var (
		start, end, prefix []byte
	)

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
			prefix = start
		}
	}

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
		// if params.View == "" {
		// 	if bytes.HasPrefix(k, []byte(sep)) {
		// 		return nil
		// 	}
		// }
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
		// if strings.HasPrefix(params.View, viewsp) {
		// 	name := string(_hash([]byte(params.View[1:])))
		// 	pfx := _pat4View(name+viewx2k) + viewsp
		// 	polishedKey = bytes.TrimPrefix(polishedKey, []byte(pfx))
		// 	parts := bytes.Split(polishedKey, []byte(viewsp))
		// 	log.Printf("%s %s", parts[0], parts[1])
		// 	polishedKey = parts[0]
		// }
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
)

//-----------------------------------------------------------------------------

// // DeleteView deletes the data of a view.
// func (db *DB) DeleteView(v string) (_err error) {
// 	var cnt int
// 	var wg sync.WaitGroup
// 	for _err == nil {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			cnt, _err = db.deleteView(v)
// 			fmt.Println(cnt)
// 		}()
// 		wg.Wait()
// 		if _err != nil {
// 			return
// 		}
// 		if cnt < 1000 {
// 			return
// 		}
// 	}
// 	return
// }

// func (db *DB) deleteView(v string) (_cnt int, _err error) {
// 	log.SetFlags(log.Lshortfile)
// 	limit := 1000
// 	defer func() { _cnt = 1000 - limit }()
// 	prefix := []byte(pat(v))
// 	_err = db.db.Update(func(txn *badger.Txn) error {
// 		opt := badger.DefaultIteratorOptions
// 		opt.PrefetchValues = false
// 		itr := txn.NewIterator(opt)
// 		defer itr.Close()
// 		var todelete [][]byte
// 		for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
// 			item := itr.Item()
// 			k := item.Key()
// 			v, err := item.ValueCopy(nil)
// 			if err != nil {
// 				return err
// 			}
// 			todelete = append(todelete, k, v)
// 		}
// 		for _, vd := range todelete {
// 			log.Printf("%s %s\n", vd, prefix)
// 			if err := txn.Delete(vd); err != nil {
// 				return err
// 			}
// 			limit--
// 		}
// 		return nil
// 	})
// 	return
// }
