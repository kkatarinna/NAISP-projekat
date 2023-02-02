package structures

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

const MAX_HEIGHT = 16

type SkipList struct {
	height int
	head   *SkipListNode
}

type SkipListNode struct {
	key       string
	value     []byte
	tombstone bool
	timestamp uint64
	next      []*SkipListNode
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= MAX_HEIGHT-1 {
			if level > s.height {
				s.height = level
			}
			return level
		}
	}
	if level > s.height {
		s.height = level
	}
	return level
}

// pravljenje skip list:
// max visina - koliko niova najvse moze da ima (int)
// size - koliko elemenata moze da sadrzi (int)
// head skip liste pokazuje na Prvi Node
// koji u sebi sadrzi "sentinel" kao kljuc
// vrednost 0 tipa niza dva bajta
// i niz pokazivaca, duzine maxHeight, na druge cvorove koji su postavljeni na nil
func CreateSkipList() *SkipList {
	next := make([]*SkipListNode, MAX_HEIGHT, MAX_HEIGHT)
	value := make([]byte, 2)
	binary.LittleEndian.PutUint16(value, 0)
	timestamp := time.Now().Unix()
	sln := SkipListNode{key: "sentinel", timestamp: uint64(timestamp), tombstone: false, value: value, next: next}
	sl := SkipList{height: 0, head: &sln}
	return &sl
}

// dodavanje cvora kljuca string i vrednosti []byte
// valjda vi morate da pretvorite sta god hocete da ubacite ovde u []byte
// vratice false ako vec postoji element
func (sl SkipList) Add(key string, value []byte) bool {
	found := sl.find(key)
	if found != nil {
		if found.tombstone == false {
			return false
		}
	}
	level := sl.roll()
	next := make([]*SkipListNode, level+1)
	node := SkipListNode{key: key, tombstone: false, timestamp: uint64(time.Now().Unix()), value: value, next: next}
	here := sl.head
	i := 1
	for {
		//pocetak i kraj skip liste moze da bude nil
		if here.next[len(here.next)-i] == nil {
			if level >= len(here.next)-i {
				here.next[len(here.next)-i] = &node
			}
			i++
			if i > len(here.next) {
				return true
			}
			continue
		}
		//ako nije pocetak ili kraj liste
		if here.next[len(here.next)-i] != nil {
			//ako je kljuc novog elemnta veci predji na sledeci elemnti
			if key > here.next[len(here.next)-i].key {
				here = here.next[len(here.next)-i]
				i = 1
				continue
			}
			if key == here.next[len(here.next)-i].key {
				here.next[len(here.next)-i].tombstone = false
				here.next[len(here.next)-i].value = value
				return true
			}
			//ako je kljuc manji prevezi za taj nivo ako je potrebno i spusti se nivo
			if key < here.next[len(here.next)-i].key {
				if level >= len(here.next)-i {
					node.next[len(here.next)-i] = here.next[len(here.next)-i]
					here.next[len(here.next)-i] = &node
				}
				i++
			}
		}
		if i > len(here.next) {
			return true
		}
	}
}

// trazi cvor sa kljucem key
// vraca pokazivac na taj cvor ili nil ako ga ne nadje
func (sl SkipList) find(key string) *SkipListNode {
	here := sl.head
	i := 1
	for {
		if here.next[len(here.next)-i] == nil {
			i++
			if i > len(here.next) {
				return nil
			}
			continue
		}
		if key < here.next[len(here.next)-i].key {
			i++
			if i > len(here.next) {
				return nil
			}
			continue
		}
		if key > here.next[len(here.next)-i].key {
			here = here.next[len(here.next)-i]
			i = 1
			continue
		}
		if key == here.next[len(here.next)-i].key {
			return here.next[len(here.next)-i]
		}
		if here.key > key && here != sl.head {
			return nil
		}
		if i > MAX_HEIGHT {
			return nil
		}
	}
}

// funkcija za brisanje elementa
// vratice nil ako nije pronasao element
// vratice true ako izbrise element
func (sl SkipList) delete(key string) bool {
	if sl.find(key) == nil {
		return false
	}
	here := sl.head
	i := 1
	for {
		//pocetak i kraj skip liste moze da bude nil
		if here.next[len(here.next)-i] == nil {
			i++
			if i > len(here.next) {
				return true
			}
			continue
		}
		if here.next[len(here.next)-i] != nil {
			//ako je kljuc novog elemnta veci predji na sledeci elemnti
			if key > here.next[len(here.next)-i].key {
				here = here.next[len(here.next)-i]
				i = 1
				continue
			}
			//ako je kljuc manji prevezi za taj nivo ako je potrebno i spusti se nivo
			if key < here.next[len(here.next)-i].key {
				i++
				if i > len(here.next) {
					return true
				}
				continue
			}
			if key == here.next[len(here.next)-i].key {
				here.next[len(here.next)-i] = here.next[len(here.next)-i].next[len(here.next)-i]
			}
		}
		if i > len(here.next) {
			return true
		}
	}
}

// funkcija za brisanje elementa
// vratice nil ako nije pronasao element
// vratice true ako izbrise element
func (sl SkipList) logicDelete(key string) bool {
	if sl.find(key) == nil {
		return false
	}
	here := sl.head
	i := 1
	for {
		//pocetak i kraj skip liste moze da bude nil
		if here.next[len(here.next)-i] == nil {
			i++
			if i > len(here.next) {
				return true
			}
			continue
		}
		if here.next[len(here.next)-i] != nil {
			//ako je kljuc novog elemnta veci predji na sledeci elemnti
			if key > here.next[len(here.next)-i].key {
				here = here.next[len(here.next)-i]
				i = 1
				continue
			}
			//ako je kljuc manji prevezi za taj nivo ako je potrebno i spusti se nivo
			if key < here.next[len(here.next)-i].key {
				i++
				continue
			}
			if key == here.next[len(here.next)-i].key {
				here.next[len(here.next)-i].timestamp = uint64(time.Now().Unix())
				here.next[len(here.next)-i].tombstone = true
				return true
			}
		}
	}
}

func (sl SkipList) print() *[]*SkipListNode {

	list := make([]*SkipListNode, 0)

	here := sl.head
	for {
		if here.next[0] != nil {
			here = here.next[0]
		} else {
			break
		}
		list = append(list, here)
		fmt.Printf("adresa:%p, key:%s, tombstone:%v, timestamp:%v, value:%v, next:%v\n", here, here.key, here.tombstone, here.timestamp, here.value, here.next)
	}
	fmt.Println()

	return &list
}
