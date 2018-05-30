package dockage

import (
	"hash/fnv"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/fatih/structs"
)

func prepdoc(doc interface{}) (resID []byte, resRev *structs.Field, reserr error) {
	ins := new(inspector)
	ins.inspect(doc)
	if ins.id == "" {
		reserr = ErrNoID
		return
	}
	sid := ins.id
	if strings.ContainsAny(sid, specials) {
		reserr = ErrInvalidID
		return
	}
	resID = []byte(sid)
	if ins.rev == nil {
		reserr = ErrNoRev
		return
	}
	resRev = ins.rev
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

func pat4Sys(s ...string) string {
	return syssp + strings.Join(s, syssp)
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

func stopWords(params Q, forIndexedKeys ...bool) (start, end, prefix []byte) {
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
		domain := viewx2k
		if len(forIndexedKeys) > 0 && forIndexedKeys[0] {
			domain = viewk2x
		}
		pfx := pat4View(name + domain)
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

type inspector struct {
	id  string
	rev *structs.Field
}

func (ins *inspector) inspect(v interface{}) {
	list := structs.Fields(v)
	ins.recinspect(list...)
}

func (ins *inspector) recinspect(fields ...*structs.Field) {
	for _, fl := range fields {
		if fl.IsEmbedded() {
			ins.recinspect(fl.Fields()...)
			continue
		}
		dok := fl.Tag("dok")
		switch dok {
		case "id":
			ins.id = fl.Value().(string)
		case "rev":
			ins.rev = fl
		}
	}
}
