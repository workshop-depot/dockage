package dockage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/tidwall/gjson"
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
	_db, err := Open(opts)
	if err != nil {
		panic(err)
	}

	return _db
}

func ExampleDB() {
	db := createDB()
	defer db.Close()

	cmnt := comment{
		ID:   "CMNT::001",
		By:   "Frodo Baggins",
		Text: "Hi!",
		At:   time.Now().UnixNano(),
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

func ExampleDB_Delete() {
	db := createDB()
	defer db.Close()

	cmnt := comment{
		ID:   "CMNT::001",
		By:   "Frodo Baggins",
		Text: "Hi!",
		At:   time.Now().UnixNano(),
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

	db.AddView(NewView("tags",
		func(em Emitter, k, v []byte) {
			type kv = KV
			res := gjson.Get(string(v), "tags")
			res.ForEach(func(pk, pv gjson.Result) bool {
				em.Emit([]byte(pv.String()), nil)
				return true
			})
			return
		}))

	var list []interface{}
	for i := 1; i <= 3; i++ {
		cmnt := comment{
			ID:   fmt.Sprintf("CMNT::%03d", i),
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now().UnixNano(),
			Tags: []string{"tech", "golang"},
		}
		list = append(list, cmnt)
	}
	fmt.Println(db.Put(list...))

	res, err := db.Query(Q{View: "tags", Start: []byte("tech")})
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
