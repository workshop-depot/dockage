package dockage

import (
	"encoding/json"
	"hash/fnv"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/tidwall/gjson"
)

func prepdoc(doc interface{}) (resjs, resID []byte, reserr error) {
	switch x := doc.(type) {
	case string:
		resjs = []byte(x)
	case []byte:
		resjs = x
	default:
		js, err := json.Marshal(doc)
		if err != nil {
			reserr = err
			return
		}
		resjs = js
	}
	if !gjson.Valid(string(resjs)) {
		reserr = ErrInvalidJSONDoc
		return
	}
	resid := gjson.Get(string(resjs), "id")
	if !resid.Exists() {
		reserr = ErrNoID
		return
	}
	sid := resid.String()
	if strings.ContainsAny(sid, specials) {
		reserr = ErrInvalidID
		return
	}
	resID = []byte(sid)
	return
}

func fnvhash(v []byte) []byte {
	h := fnv.New64a()
	h.Write(v)
	return h.Sum(nil)
}

func pat4View(s ...string) string {
	return viewsp + strings.Join(s, viewsp)
}

func pat4Key(s ...string) string {
	return keysp + strings.Join(s, keysp)
}

func getlimits(params Q) (skip, limit int, applySkip, applyLimit bool) {
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

func stopWords(params Q) (start, end, prefix []byte) {
	if params.View == "" {
		start = []byte(pat4Key(string(params.Start)))
		if len(params.End) > 0 {
			end = []byte(pat4Key(string(params.End)))
		}
		if len(params.Prefix) > 0 {
			prefix = []byte(pat4Key(string(params.Prefix)))
		} else {
			prefix = start
		}
	} else {
		name := string(fnvhash([]byte(params.View)))
		pfx := pat4View(name + viewx2k)
		start = []byte(pfx + pat4View(string(params.Start)))
		if len(params.End) > 0 {
			end = []byte(pfx + pat4View(string(params.End)))
		}
		if len(params.Prefix) > 0 {
			prefix = []byte(pfx + pat4View(string(params.Prefix)))
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
