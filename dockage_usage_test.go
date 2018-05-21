package dockage_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dc0d/dockage"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

var (
	db *dockage.DB
)

func mkdir(d string) {
	if err := os.MkdirAll(d, 0777); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
}

func initdb() {
	databaseDir, _ := ioutil.TempDir(os.TempDir(), "database")
	{
		stat, err := os.Stat(databaseDir)
		if err != nil {
			if !os.IsNotExist(err) {
				panic(err)
			}
		}
		if stat != nil {
			if err := os.RemoveAll(databaseDir); err != nil {
				panic(err)
			}
		}
	}
	index := filepath.Join(databaseDir, "index")
	data := filepath.Join(databaseDir, "data")

	mkdir(index)
	mkdir(data)

	var opts dockage.Options
	opts.Dir = index
	opts.ValueDir = data
	_db, err := dockage.Open(opts)
	if err != nil {
		panic(err)
	}
	db = _db
}

func TestMain(m *testing.M) {
	initdb()
	code := m.Run()
	db.Close()
	os.Exit(code)
}

type data struct {
	ID   string   `json:"id,omitempty"`
	Text string   `json:"text,omitempty"`
	At   int64    `json:"at,omitempty"`
	Tags []string `json:"tags,omitempty"`
}

type Q = dockage.Q

func TestSmoke(t *testing.T) {
	require := require.New(t)

	ddb := db

	var ids []string
	for i := 1; i <= 15; i++ {
		k := fmt.Sprintf("D%06d", i)
		ids = append(ids, k)
	}
	require.NoError(ddb.Delete(ids...))

	var list []interface{}
	for i := 1; i <= 15; i++ {
		k, v := fmt.Sprintf("D%06d", i), fmt.Sprintf("V%06d", i)
		list = append(list, data{ID: k, Text: v, At: time.Now().UnixNano()})
	}
	require.NoError(ddb.Put(list...))

	l, err := ddb.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(15, len(l))

	l, err = ddb.Query(Q{Start: []byte("D00001")})
	require.NoError(err)
	require.Equal(6, len(l))

	l, err = ddb.Query(Q{Start: []byte("D00001"), Limit: 2})
	require.NoError(err)
	require.Equal(2, len(l))

	l, err = ddb.Query(Q{Start: []byte("D00001"), End: []byte("D000012")})
	require.NoError(err)
	require.Equal(3, len(l))

	l, err = ddb.Query(Q{Start: []byte("D000012"), Prefix: []byte("D00001")})
	require.NoError(err)
	require.Equal(4, len(l))

	l, err = ddb.Query(Q{Start: []byte("D000012"), Prefix: []byte("D00001"), Skip: 1, Limit: 2})
	require.NoError(err)
	require.Equal(2, len(l))
}

func TestView(t *testing.T) {
	require := require.New(t)

	ddb := db
	ddb.AddView(dockage.NewView(
		"tags",
		func(k, v []byte) (_res []dockage.KV) {
			type kv = dockage.KV
			res := gjson.Get(string(v), "tags")
			res.ForEach(func(pk, pv gjson.Result) bool {
				_res = append(_res, kv{Key: []byte(pv.String())})
				return true
			})
			return
		}))

	var ids []string
	for i := 1; i <= 150; i++ {
		k := fmt.Sprintf("D%06d", i)
		ids = append(ids, k)
	}
	require.NoError(ddb.Delete(ids...))

	var list []interface{}
	for i := 1; i <= 5; i++ {
		k, v := fmt.Sprintf("D%06d", i), fmt.Sprintf("V%06d", i)
		d := data{ID: k, Text: v, At: time.Now().UnixNano()}
		for j := 1; j <= 3; j++ {
			d.Tags = append(d.Tags, fmt.Sprintf("TAG%03d", j))
		}
		list = append(list, d)
	}
	require.NoError(ddb.Put(list...))

	l, err := ddb.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(5, len(l))

	l, err = ddb.Query(Q{View: "tags", End: []byte("\uffff")})
	require.NoError(err)
	require.Equal(15, len(l))

	l, err = ddb.Query(Q{View: "tags", Start: []byte("TAG002"), End: []byte("\uffff")})
	require.NoError(err)
	require.Equal(5, len(l))

	l, err = ddb.Query(Q{View: "tags", Start: []byte("TAG002"), Prefix: []byte("TAG00"), End: []byte("\uffff")})
	require.NoError(err)
	require.Equal(10, len(l))
}
