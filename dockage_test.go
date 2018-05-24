package dockage

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// GOCACHE=off

func TestHash(t *testing.T) {
	require := require.New(t)

	term := "1699dc18-e717-4875-9cea-d736ce3dfa05"
	h := _hash([]byte(term))
	p := hex.EncodeToString(h)
	require.Equal("1116235dbc10f81b", p)
}

func TestPat4Veiw(t *testing.T) {
	require := require.New(t)

	require.Equal(viewsp, _pat4View())
	require.Equal(viewsp+"KEY", _pat4View("KEY"))
	require.Equal(viewsp+"KEY"+viewsp+"PART", _pat4View("KEY", "PART"))
}

var (
	db *DB
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

	var opts Options
	opts.Dir = index
	opts.ValueDir = data
	_db, err := Open(opts)
	if err != nil {
		panic(err)
	}
	db = _db
}

func TestMain(m *testing.M) {
	initdb()
	var code int
	func() {
		defer db.Close()
		code = m.Run()
	}()
	os.Exit(code)
}

type comment struct {
	ID   string   `json:"id,omitempty"`
	By   string   `json:"by,omitempty"`
	Text string   `json:"text,omitempty"`
	At   int64    `json:"at,omitempty"`
	Tags []string `json:"tags,omitempty"`
}

func TestPutDelete(t *testing.T) {
	require := require.New(t)

	for i := 0; i < 10; i++ {
		n := rand.Intn(1000)
		testPutDelete(n, require)
	}
}

func testPutDelete(n int, require *require.Assertions) {
	var list []interface{}
	for i := 1; i <= n; i++ {
		k, v := fmt.Sprintf("D%06d", i), fmt.Sprintf("V%06d", i)
		list = append(list, comment{ID: k, Text: v, At: time.Now().UnixNano()})
	}
	require.NoError(db.Put(list...))

	var ids []string
	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("D%06d", i)
		ids = append(ids, k)
	}
	require.NoError(db.Delete(ids...))

	l, err := db._all()
	require.NoError(err)
	require.Equal(0, len(l))
}

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
		list = append(list, comment{ID: k, Text: v, At: time.Now().UnixNano()})
	}
	require.NoError(ddb.Put(list...))

	l, err := ddb.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(15, len(l))
	for i := 0; i < len(ids); i++ {
		require.Equal(ids[i], string(l[i].Key))
	}

	l, err = ddb.Query(Q{Start: []byte("D00001")})
	require.NoError(err)
	require.Equal(6, len(l))
	for i := 10; i <= 15; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-10].Key))
	}

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
	ddb.AddView(NewView(
		"tags",
		func(em Emitter, k, v []byte) {
			type kv = KV
			res := gjson.Get(string(v), "tags")
			res.ForEach(func(pk, pv gjson.Result) bool {
				em.Emit([]byte(pv.String()), nil)
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
		d := comment{ID: k, Text: v, At: time.Now().UnixNano()}
		for j := 1; j <= 3; j++ {
			d.Tags = append(d.Tags, fmt.Sprintf("TAG%03d", j))
		}
		list = append(list, d)
	}
	require.NoError(ddb.Put(list...))

	l, err := ddb.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(5, len(l))
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-1].Key))
	}

	l, err = ddb.Query(Q{View: "tags"})
	require.NoError(err)
	require.Equal(15, len(l))
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-1].Key))
	}

	l, err = ddb.Query(Q{View: "tags", Start: []byte("TAG002")})
	require.NoError(err)
	require.Equal(5, len(l))

	l, err = ddb.Query(Q{View: "tags", Start: []byte("TAG002"), Prefix: []byte("TAG00")})
	require.NoError(err)
	require.Equal(10, len(l))
	for i := 1; i <= 10; i++ {
		j := i
		if j > 5 {
			j = j - 5
			require.Equal("TAG003", string(l[i-1].Index))
		} else {
			require.Equal("TAG002", string(l[i-1].Index))
		}
		k := fmt.Sprintf("D%06d", j)
		require.Equal(k, string(l[i-1].Key))
	}
}

func TestDeleteView(t *testing.T) {
	require := require.New(t)

	var ids []string
	for i := 1; i <= 150; i++ {
		k := fmt.Sprintf("D%06d", i)
		ids = append(ids, k)
	}
	require.NoError(db.Delete(ids...))

	N := 631
	var list []interface{}
	for i := 1; i <= N; i++ {
		k, v := fmt.Sprintf("D%06d", i), fmt.Sprintf("V%06d", i)
		d := comment{ID: k, Text: v, At: time.Now().UnixNano()}
		for j := 1; j <= 3; j++ {
			d.Tags = append(d.Tags, fmt.Sprintf("TAG%03d", j))
		}
		list = append(list, d)
	}
	require.NoError(db.Put(list...))

	l, err := db.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(N, len(l))
	for i := 1; i <= N; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-1].Key))
	}

	lkv, err := db._all()
	require.NoError(err)
	require.Equal(N+N*3+N*3, len(lkv))

	require.NoError(db.DeleteView("tags"))

	lkv, err = db._all()
	require.NoError(err)
	require.Equal(N, len(lkv))
}
