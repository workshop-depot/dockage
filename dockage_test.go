package dockage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// GOCACHE=off

func TestHash(t *testing.T) {
	require := require.New(t)

	term := "1699dc18-e717-4875-9cea-d736ce3dfa05"
	h := fnvhash([]byte(term))
	p := hex.EncodeToString(h)
	require.Equal("1116235dbc10f81b", p)
}

func TestPat4Veiw(t *testing.T) {
	require := require.New(t)

	require.Equal(viewsp, pat4View())
	require.Equal(viewsp+"KEY", pat4View("KEY"))
	require.Equal(viewsp+"KEY"+viewsp+"PART", pat4View("KEY", "PART"))
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
	preppedDB, err := Open(opts)
	if err != nil {
		panic(err)
	}
	db = preppedDB
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
	ID   string    `json:"id"`
	Rev  string    `json:"rev"`
	By   string    `json:"by,omitempty"`
	Text string    `json:"text,omitempty"`
	At   time.Time `json:"at,omitempty"`
	Tags []string  `json:"tags,omitempty"`
}

func TestPutDelete(t *testing.T) {
	require := require.New(t)

	wg := new(sync.WaitGroup)
	for i := 0; i < 100; i++ {
		i := i
		n := rand.Intn(100)
		wg.Add(1)
		go testPutDelete(wg, i*100, n, require)
	}

	wg.Wait()

	l, err := db.unboundAll()
	require.NoError(err)
	require.Equal(0+1 /* dbseq */, len(l))
}

func testPutDelete(wg *sync.WaitGroup, start, n int, require *require.Assertions) {
	defer wg.Done()
	var list []interface{}
	for i := start; i <= start+n; i++ {
		k, v := fmt.Sprintf("D%06d", i), fmt.Sprintf("V%06d", i)
		list = append(list, comment{ID: k, Text: v, At: time.Now()})
	}
	require.NoError(db.Put(list...))

	var ids []string
	for i := start; i <= start+n; i++ {
		k := fmt.Sprintf("D%06d", i)
		ids = append(ids, k)
	}
	require.NoError(db.Delete(ids...))
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
		list = append(list, comment{ID: k, Text: v, At: time.Now()})
	}
	require.NoError(ddb.Put(list...))

	l, _, err := ddb.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(15, len(l))
	for i := 0; i < len(ids); i++ {
		require.Equal(ids[i], string(l[i].Key))
	}

	l, _, err = ddb.Query(Q{Start: []byte("D00001")})
	require.NoError(err)
	require.Equal(6, len(l))
	for i := 10; i <= 15; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-10].Key))
	}

	l, _, err = ddb.Query(Q{Start: []byte("D00001"), Limit: 2})
	require.NoError(err)
	require.Equal(2, len(l))

	l, _, err = ddb.Query(Q{Start: []byte("D00001"), End: []byte("D000012")})
	require.NoError(err)
	require.Equal(3, len(l))

	l, _, err = ddb.Query(Q{Start: []byte("D000012"), Prefix: []byte("D00001")})
	require.NoError(err)
	require.Equal(4, len(l))

	l, _, err = ddb.Query(Q{Start: []byte("D000012"), Prefix: []byte("D00001"), Skip: 1, Limit: 2})
	require.NoError(err)
	require.Equal(2, len(l))
}

func TestView(t *testing.T) {
	require := require.New(t)

	ddb := db
	ddb.AddView(NewView(
		"tags",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(comment)
			if !ok {
				return
			}
			for _, v := range c.Tags {
				em.Emit([]byte(v), nil)
			}
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
		d := comment{ID: k, Text: v, At: time.Now()}
		for j := 1; j <= 3; j++ {
			d.Tags = append(d.Tags, fmt.Sprintf("TAG%03d", j))
		}
		list = append(list, d)
	}
	require.NoError(ddb.Put(list...))

	l, cnt, err := ddb.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(5, len(l))
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-1].Key))
	}

	l, _, err = ddb.Query(Q{View: "tags"})
	require.NoError(err)
	require.Equal(15, len(l))
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-1].Key))
	}

	l, cnt, err = ddb.Query(Q{View: "tags", Start: []byte("TAG002")})
	require.NoError(err)
	require.Equal(10, len(l))
	require.Equal(10, cnt)

	l, _, err = ddb.Query(Q{View: "tags", Start: []byte("TAG002"), Prefix: []byte("TAG00")})
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
		d := comment{ID: k, Text: v, At: time.Now()}
		for j := 1; j <= 3; j++ {
			d.Tags = append(d.Tags, fmt.Sprintf("TAG%03d", j))
		}
		list = append(list, d)
	}
	require.NoError(db.Put(list...))

	l, _, err := db.Query(Q{Limit: 1000000})
	require.NoError(err)
	require.Equal(N, len(l))
	for i := 1; i <= N; i++ {
		k := fmt.Sprintf("D%06d", i)
		require.Equal(k, string(l[i-1].Key))
	}

	l, _, err = db.Query(Q{Limit: 1000000, View: "tags"})
	require.NoError(err)
	require.Equal(N*3, len(l))

	require.NoError(db.DeleteView("tags"))

	l, _, err = db.Query(Q{Limit: 1000000, View: "tags"})
	require.NoError(err)
	require.Equal(0, len(l))
}

func TestRevPut(t *testing.T) {
	require := require.New(t)

	c := &comment{ID: "C4", Text: "Hi!"}

	require.NoError(db.Put(c))

	{
		var res []comment
		err := db.Get(&res, "C4")
		require.NoError(err)
		require.Equal(1, len(res))
		fst := res[0]
		require.Equal("C4", string(fst.ID))
		*c = fst
	}

	rev1 := c.Rev

	c.Rev = "QQ"
	require.Equal(ErrNoMatchRev, db.Put(c))

	c.Rev = ""
	require.Equal(ErrNoMatchRev, db.Put(c))

	{
		var res []comment
		err := db.Get(&res, "C4")
		require.NoError(err)
		require.Equal(1, len(res))
		fst := res[0]
		require.Equal("C4", string(fst.ID))
		*c = fst
	}

	c.Text = "EDIT 01"
	require.NoError(db.Put(c))

	{
		var res []comment
		err := db.Get(&res, "C4")
		require.NoError(err)
		require.Equal(1, len(res))
		fst := res[0]
		require.Equal("C4", string(fst.ID))
		*c = fst
	}
	require.Equal("EDIT 01", c.Text)

	rev2 := c.Rev

	require.True(bytes.Compare([]byte(rev2), []byte(rev1)) > 0)
}

func TestRevPut2(t *testing.T) {
	require := require.New(t)

	require.NoError(db.Delete("C4"))

	c := &comment{ID: "C4"}
	var prevRev []byte
	for i := 0; i < 10; i++ {
		c.Text = fmt.Sprintf("Hi! %d", i)
		require.NoError(db.Put(c))
		var res []comment
		err := db.Get(&res, "C4")
		require.NoError(err)
		require.Equal(1, len(res))
		fst := res[0]
		require.Equal("C4", string(fst.ID))
		rev := []byte(fst.Rev)
		if len(prevRev) > 0 {
			require.True(bytes.Compare(rev, prevRev) > 0)
		}
		prevRev = rev
	}
}

type Granny struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
}

type Daddy struct {
	Granny
	UpdatedAt time.Time
}

type Data struct {
	Daddy
	Text string
}

func TestInspector(t *testing.T) {
	require := require.New(t)

	dt := &Data{}
	dt.ID = "100"

	var v interface{} = dt

	ins := new(inspector)
	ins.inspect(v)
	ins.rev.Set("R-100")

	require.Equal("100", ins.id)
	require.Equal("R-100", ins.rev.Value())

	g := Granny{ID: "100", Rev: "R-100"}

	v = g

	ins = new(inspector)
	ins.inspect(v)
	ins.rev.Set("R-100")

	require.Equal("100", ins.id)
	require.Equal("R-100", ins.rev.Value())
}

func TestGet2(t *testing.T) {
	require := require.New(t)
	db := createDB()
	defer db.Close()

	var list []interface{}
	var ids []string
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		ids = append(ids, cmnt.ID)
	}
	require.NoError(db.Put(list...))

	var result []comment
	require.NoError(db.Get(&result, ids[0], ids[1:]...))
	require.Equal(3, len(result))
	for _, v := range result {
		require.Equal("Frodo Baggins", v.By)
	}
}

func TestGetDifferentDocs(t *testing.T) {
	require := require.New(t)
	db := createDB()
	defer db.Close()

	var clist []interface{}
	var cids []string
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"tech", "golang"},
		}
		clist = append(clist, cmnt)
		cids = append(cids, cmnt.ID)
	}
	require.NoError(db.Put(clist...))

	type post struct {
		ID     string `json:"id"`
		Rev    string `json:"rev"`
		Author string `json:"by,omitempty"`
		Text   string `json:"text,omitempty"`
	}

	var plist []interface{}
	var pids []string
	for i := 1; i <= 3; i++ {
		p := &post{
			ID:     fmt.Sprintf("POST::%03d", i),
			Author: "Ta Da!",
			Text:   "Hya!",
		}
		plist = append(plist, p)
		pids = append(pids, p.ID)
	}
	require.NoError(db.Put(plist...))

	// get documents of different types different
	var result = []interface{}{
		&comment{},
		&post{},
	}
	require.NoError(db.Get(&result, "CMNT::001", "POST::002"))
	require.Equal(2, len(result))
	for _, v := range result {
		switch x := v.(type) {
		case *comment:
			require.Equal("Frodo Baggins", x.By)
		case *post:
			require.Equal("Ta Da!", x.Author)
			require.Equal("Hya!", x.Text)
		default:
			require.Failf("UNKNOWN TYPE", fmt.Sprintf("%T", v))
		}
	}
}
