package structures

import (
	"container/list"
)

type Cache struct {
    capacity int
    list     *list.List
    items    map[string]list.Element
}

type Entry struct { //kao neka struktura za podatke, moze da se izmeni ako treba
    key   string
    Value []byte
}

func NewCache(capacity int) *Cache { //konstruktor za cache
    return &Cache{
        capacity: capacity,
        list:     list.New(),
        items:    make(map[string]list.Element),
    }
}

func (c Cache) Set(key string, value []byte) {
    if elem, ok := c.items[key]; ok { //stavlja element ispred svih u ;isti ukoliko postoji vec element u njoj
        c.list.MoveToFront(&elem)
        elem.Value.(*Entry).Value = value
        return
    }
    e := c.list.PushFront(&Entry{key, value}) //samo postavi novi element
    c.items[key] = *e
    if c.list.Len() > c.capacity {
        c.RemoveOldest()
    }
}

func (c Cache) Get(key string) ([]byte, bool) {
    if e, ok := c.items[key]; ok {
        c.list.MoveToFront(&e)
        return e.Value.(Entry).Value, true
    }
    return nil, false
}

func (c Cache) RemoveOldest() {
    e := c.list.Back()
    if e != nil {
        c.list.Remove(e)
        kv := e.Value.(Entry)
        delete(c.items, kv.key)
    }
}