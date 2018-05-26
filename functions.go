package dockage

import (
	"encoding/json"
	"hash/fnv"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/tidwall/gjson"
)

func _prep(doc interface{}) (_js, _id []byte, _err error) {
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
	sid := resid.String()
	if strings.ContainsAny(sid, specials) {
		_err = ErrInvalidID
		return
	}
	_id = []byte(sid)
	return
}

func _hash(v []byte) []byte {
	h := fnv.New64a()
	h.Write(v)
	return h.Sum(nil)
}

func _pat4View(s ...string) string {
	return viewsp + strings.Join(s, viewsp)
}

func _pat4Key(s ...string) string {
	return keysp + strings.Join(s, keysp)
}

func _limits(params Q) (skip, limit int, applySkip, applyLimit bool) {
	skip = params.Skip
	limit = params.Limit
	var ()
	if skip > 0 {
		applySkip = true
	}
	if limit <= 0 && !params.Count {
		limit = 100
	}
	if limit > 0 {
		applyLimit = true
	}
	return
}

func _stopWords(params Q) (start, end, prefix []byte) {
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

func _itrFunc(txn *badger.Txn,
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
