# dockage
This is an embedded document/json store based on [badger](https://github.com/dgraph-io/badger) key-value store - WIP, alpha quality.

### TODO

- [x] put document
- [x] delete document
- [x] query based on predefined views - like CouchDB
- [ ] delete view
- [ ] rebuild view
- [ ] query count
- [ ] cas
- [ ] if end ends in \uffff make it the same length as start
- [ ] polish/check id (no ^, < or >)
