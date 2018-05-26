[![Go Report Card](https://goreportcard.com/badge/github.com/dc0d/dockage)](https://goreportcard.com/report/github.com/dc0d/dockage) [![GoDoc](https://godoc.org/github.com/dc0d/dockage?status.svg)](https://godoc.org/github.com/dc0d/dockage)

# dockage
This is an embedded document/json store based on [badger](https://github.com/dgraph-io/badger) key-value store - WIP, alpha quality.

# intro

Views play the role of indices. In a tradition RDBMS there are only predefined indices: a numeric index, a time index, a text index, etc.

Views allow to define indices based on any characteristics of the data - not just based on numeric, time or boolean fields. Nested data too, can be indexed in various ways, employing views.

For example we need to put some comments into the database:

```go
type comment struct {
	ID   string    `json:"id,omitempty"`
	By   string    `json:"by,omitempty"`
	Text string    `json:"text,omitempty"`
	At   time.Time `json:"at,omitempty"`
	Tags []string  `json:"tags,omitempty"`
}
```

And we want to find comments based on their tags. For this, a view is needed that emits tags:

```go
db.AddView(NewView("tags",
    func(em Emitter, k, v []byte) {
        type kv = KV
        res := gjson.Get(string(v), "tags")
        if !res.Exists() {
            return
        }
        res.ForEach(func(pk, pv gjson.Result) bool {
            em.Emit([]byte(pv.String()), nil)
            return true
        })
        return
    }))
```

As it can be seen, for building this view, first we need to extract tags. [`gjson`](https://github.com/tidwall/gjson/) is an awesome package that allows us to access different parts of a json string without unmarshalling it to a Go struct.

Using `gjson` we get the `tags` json field by `res := gjson.Get(string(v), "tags")` and if the document has such field, we emit each tag to the view.

Now to find all comments that have a `tech` tag, the view can be queried by `db.Query(Q{View: "tags", Start: []byte("tech")})`.