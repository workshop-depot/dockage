package dockage

import (
	"encoding/json"
	"hash/fnv"
	"strings"

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
	_id = []byte(resid.String())
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
