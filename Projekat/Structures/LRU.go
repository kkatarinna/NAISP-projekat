package structures

import (
	"container/list"
	"fmt"
)

type Cache struct {
	capacity int
	list     *list.List
	items    map[int]*list.Element
}

type Entry struct { //kao neka struktura za podatke, moze da se izmeni ako treba
	key   int
	value int
}

func NewCache(capacity int) *Cache { //konstruktor za cache
	return &Cache{
		capacity: capacity,
		list:     list.New(),
		items:    make(map[int]*list.Element),
	}
}

func (c *Cache) Set(key int, value int) {
	if elem, ok := c.items[key]; ok { //stavlja element ispred svih u ;isti ukoliko postoji vec element u njoj
		c.list.MoveToFront(elem)
		elem.Value.(*Entry).value = value
		return
	}
	e := c.list.PushFront(&Entry{key, value}) //samo postavi novi element
	c.items[key] = e
	if c.list.Len() > c.capacity {
		c.RemoveOldest()
	}
}

func (c *Cache) Get(key int) (int, bool) {
	if e, ok := c.items[key]; ok {
		c.list.MoveToFront(e)
		return e.Value.(*Entry).value, true
	}
	return 0, false
}

func (c *Cache) RemoveOldest() {
	e := c.list.Back()
	if e != nil {
		c.list.Remove(e)
		kv := e.Value.(*Entry)
		delete(c.items, kv.key)
	}
}

func main() {
	c := NewCache(3)
	c.Set(1, 1)
	c.Set(2, 2)
	c.Set(2, 2)
	fmt.Println(c.Get(1))
	c.Set(2, 2)
	fmt.Println(c.Get(1))
	fmt.Println("========")
	c.Set(3, 3)
	fmt.Println(c.Get(2))
	c.Set(4, 4)
	fmt.Println(c.Get(1))
	fmt.Println(c.Get(3))
	fmt.Println(c.Get(4))
}
