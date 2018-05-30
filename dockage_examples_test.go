package dockage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

func createDB() *DB {
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

	return preppedDB
}

func ExampleDB_Put() {
	db := createDB()
	defer db.Close()

	cmnt := comment{
		ID:   "CMNT::001",
		By:   "Frodo Baggins",
		Text: "Hi!",
		At:   time.Now(),
		Tags: []string{"tech", "golang"},
	}

	fmt.Println(db.Put(cmnt))

	res, err := db.Get("CMNT::001")
	fmt.Println(err)

	fmt.Println(json.Unmarshal(res[0].Val, &cmnt))

	fmt.Println(cmnt.ID)
	fmt.Println(cmnt.By)
	fmt.Println(cmnt.Text)
	fmt.Println(cmnt.Tags)

	// Output:
	// <nil>
	// <nil>
	// <nil>
	// CMNT::001
	// Frodo Baggins
	// Hi!
	// [tech golang]
}

func ExampleDB_cas() {
	// CAS is performed using a mandatory rev field inside json document.

	db := createDB()
	defer db.Close()

	docID := "CMNT::001"

	cmnt := &comment{
		ID:   docID,
		By:   "Frodo Baggins",
		Text: "Hi!",
		At:   time.Now(),
		Tags: []string{"tech", "golang"},
	}

	fmt.Println(db.Put(cmnt))

	res, err := db.Get(docID)
	fmt.Println(err)
	fmt.Println(json.Unmarshal(res[0].Val, cmnt))

	fmt.Println(cmnt.ID)
	fmt.Println(cmnt.By)
	fmt.Println(cmnt.Text)
	fmt.Println(cmnt.Tags)
	fmt.Println(cmnt.Rev)

	rev := cmnt.Rev
	cmnt.Rev = "dummy"
	fmt.Println("error:", db.Put(cmnt))

	cmnt.Rev = rev
	cmnt.Text = "Back again!"
	fmt.Println(db.Put(cmnt))

	res, err = db.Get(docID)
	fmt.Println(err)
	fmt.Println(json.Unmarshal(res[0].Val, &cmnt))

	fmt.Println(cmnt.ID)
	fmt.Println(cmnt.By)
	fmt.Println(cmnt.Text)
	fmt.Println(cmnt.Tags)
	fmt.Println(cmnt.Rev)

	// Output:
	// <nil>
	// <nil>
	// <nil>
	// CMNT::001
	// Frodo Baggins
	// Hi!
	// [tech golang]
	// 0000000000000000
	// error: rev field in doc json not matching
	// <nil>
	// <nil>
	// <nil>
	// CMNT::001
	// Frodo Baggins
	// Back again!
	// [tech golang]
	// 0000000000000001
}

func ExampleDB_Delete() {
	db := createDB()
	defer db.Close()

	cmnt := &comment{
		ID:   "CMNT::001",
		By:   "Frodo Baggins",
		Text: "Hi!",
		At:   time.Now(),
		Tags: []string{"tech", "golang"},
	}

	fmt.Println(db.Put(cmnt))

	fmt.Println(db.Delete("CMNT::001"))

	res, err := db.Get("CMNT::001")
	fmt.Println(err, res)

	// Output:
	// <nil>
	// <nil>
	// Key not found []
}

func ExampleView() {
	db := createDB()
	defer db.Close()

	// em Emitter allows to emit our view index into the view, in this case
	// the tags of a comment. By emitting tags one by one, it is possible
	// to query comments based on their tags.

	db.AddView(NewView("tags",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			for _, v := range c.Tags {
				em.Emit([]byte(v), nil)
			}
		}))

	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
	}
	fmt.Println(db.Put(list...))

	res, _, err := db.Query(Q{View: "tags", Start: []byte("tech")})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001  tech
	// CMNT::002  tech
	// CMNT::003  tech
}

func ExampleView_byTime() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			if t.IsZero() {
				return
			}
			em.Emit([]byte(t.Format("2006-01-02")), nil)
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   at,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
	}
	fmt.Println(db.Put(list...))

	start := []byte("2018-01-01")
	prefix := []byte("2018-01")
	res, _, err := db.Query(Q{View: "by_time", Start: start, Prefix: prefix})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001  2018-01-01
	// CMNT::002  2018-01-02
	// CMNT::003  2018-01-03
}

func ExampleView_viewVal() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			if t.IsZero() {
				return
			}
			em.Emit([]byte(t.Format("2006-01-02")), []byte(c.By))
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   at,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
	}
	fmt.Println(db.Put(list...))

	start := []byte("2018-01-01")
	prefix := []byte("2018-01")
	res, _, err := db.Query(Q{View: "by_time", Start: start, Prefix: prefix})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001 Frodo Baggins 2018-01-01
	// CMNT::002 Frodo Baggins 2018-01-02
	// CMNT::003 Frodo Baggins 2018-01-03
}

func ExampleView_limit() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			if t.IsZero() {
				return
			}
			em.Emit([]byte(t.Format("2006-01-02")), nil)
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   at,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
	}
	fmt.Println(db.Put(list...))

	start := []byte("2018-01-01")
	prefix := []byte("2018-01")
	res, _, err := db.Query(Q{View: "by_time", Start: start, Prefix: prefix, Limit: 1})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001  2018-01-01
}

func ExampleView_end() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			if t.IsZero() {
				return
			}
			em.Emit([]byte(t.Format("2006-01-02")), nil)
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   at,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
	}
	fmt.Println(db.Put(list...))

	start := []byte("2018-01-01")
	prefix := []byte("2018-01")
	end := []byte("2018-01-03") // exclusive
	res, _, err := db.Query(Q{View: "by_time", Start: start, Prefix: prefix, End: end})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001  2018-01-01
	// CMNT::002  2018-01-02
}

func ExampleView_endAll() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			if t.IsZero() {
				return
			}
			em.Emit([]byte(t.Format("2006-01-02")), []byte(c.By))
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   at,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
	}
	fmt.Println(db.Put(list...))

	start := []byte("2018-01-01")
	prefix := []byte("2018-01")
	end := []byte("2018-01\uffff")
	res, _, err := db.Query(Q{View: "by_time", Start: start, Prefix: prefix, End: end})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001 Frodo Baggins 2018-01-01
	// CMNT::002 Frodo Baggins 2018-01-02
	// CMNT::003 Frodo Baggins 2018-01-03
}

func ExampleView_skip() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			if t.IsZero() {
				return
			}
			em.Emit([]byte(t.Format("2006-01-02")), []byte(c.By))
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   at,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
	}
	fmt.Println(db.Put(list...))

	start := []byte("2018-01-01")
	prefix := []byte("2018-01")
	end := []byte("2018-01\uffff")
	res, _, err := db.Query(Q{View: "by_time", Start: start, Prefix: prefix, End: end, Skip: 1})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %s\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::002 Frodo Baggins 2018-01-02
	// CMNT::003 Frodo Baggins 2018-01-03
}

func ExampleDB_Get() {
	db := createDB()
	defer db.Close()

	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
	}
	fmt.Println(db.Put(list...))

	res, err := db.Get(
		"CMNT::001",
		"CMNT::002",
		"CMNT::003")

	fmt.Println(err)

	for _, v := range res {
		var c comment
		fmt.Println(json.Unmarshal(v.Val, &c))
		fmt.Printf("%s %s %s\n", v.Key, c.Text, c.By)
	}

	// Output:
	// <nil>
	// <nil>
	// <nil>
	// CMNT::001 Hi! Frodo Baggins
	// <nil>
	// CMNT::002 Hi! Frodo Baggins
	// <nil>
	// CMNT::003 Hi! Frodo Baggins
}

func ExampleView_timestampInt64() {
	db := createDB()
	defer db.Close()

	type comment struct {
		ID   string   `json:"id"`
		Rev  string   `json:"rev"`
		By   string   `json:"by,omitempty"`
		Text string   `json:"text,omitempty"`
		At   int64    `json:"at,omitempty"`
		Tags []string `json:"tags,omitempty"`
	}

	db.AddView(NewView("by_time",
		func(em Emitter, id string, doc interface{}) {
			c, ok := doc.(*comment)
			if !ok {
				return
			}
			t := c.At
			ts := make([]byte, 8)
			binary.BigEndian.PutUint64(ts, uint64(t))
			em.Emit(ts, nil)
			return
		}))

	at := time.Date(2018, 1, 1, 12, 0, 0, 0, time.Local)
	startTS := at.Unix()
	first := startTS
	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := &comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   startTS,
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
		at = at.Add(time.Hour * 24)
		startTS = at.Unix()
	}
	fmt.Println(db.Put(list...))

	start := make([]byte, 8)
	binary.BigEndian.PutUint64(start, uint64(first))
	res, _, err := db.Query(Q{View: "by_time", Start: start})
	fmt.Println(err)

	for _, v := range res {
		fmt.Printf("%s %s %x\n", v.Key, v.Val, v.Index)
	}

	// Output:
	// <nil>
	// <nil>
	// CMNT::001  000000005a49f188
	// CMNT::002  000000005a4b4308
	// CMNT::003  000000005a4c9488
}

func ExampleView_count() {
	db := createDB()
	defer db.Close()

	db.AddView(NewView("tags",
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

	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
	}
	fmt.Println(db.Put(list...))

	res, cnt, err := db.Query(Q{View: "tags", Start: []byte("tech"), Count: true})
	fmt.Println(err)
	fmt.Println(len(res))
	fmt.Println(cnt)

	// Output:
	// <nil>
	// <nil>
	// 0
	// 3
}
