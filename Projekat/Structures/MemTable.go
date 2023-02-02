package structures

import (
	"fmt"
	. "projekat/Structures/SSTable"
	"time"
)

type Config struct {
	WalSize           uint64 `yaml:"wal_size"`
	Trashold          uint64 `yaml:"trashold"`
	MemtableSize      uint64 `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
}

type Memtable struct {
	skiplist     *SkipList
	trashold     uint64
	size         uint64
	memtableSize uint64
	walSize      uint64
}

func NewMemPar(c *Config) *Memtable {

	mem := &Memtable{skiplist: CreateSkipList(), memtableSize: c.MemtableSize, walSize: c.WalSize, trashold: c.Trashold, size: 0}
	return mem

}

func NewMem() *Memtable {

	mem := &Memtable{skiplist: CreateSkipList(), memtableSize: 10, walSize: 10, trashold: 30, size: 0}
	return mem

}

func (mem *Memtable) Insert(key string, value []byte) bool {

	node := mem.skiplist.find(key)

	if node != nil {

		node.tombstone = false
		node.value = value
		node.timestamp = uint64(time.Now().Unix())
		node.tombstone = false

	} else {
		mem.skiplist.Add(key, value)
		mem.size++
	}

	if float64(mem.size) >= float64((mem.memtableSize*mem.trashold)/100.0) {
		mem.Flush()
	}

	return true
}

func (mem *Memtable) Find(key string) []byte {

	node := mem.skiplist.find(key)
	var rec *Record

	if node != nil {

		return node.value

	} else {
		rec = Find_record_Files(key)
	}

	if rec != nil {

		return rec.Value

	} else {
		fmt.Println("Nema")
		return nil
	}

}

func (mem *Memtable) Delete(key string) {

	node := mem.skiplist.find(key)

	if node != nil {

		if !node.tombstone {
			node.tombstone = true
		}
	} else {
		mem.skiplist.Add(key, make([]byte, 0))
		mem.skiplist.logicDelete(key)
		mem.size++
	}

}

func (mem *Memtable) Flush() {

	sst := NewSSTable()

	fmt.Println(mem.size)
	listNode := mem.skiplist.print()

	listRec := make([]*Record, 0)

	for _, element := range *listNode {

		rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
		listRec = append(listRec, rec)

	}

	sst.Write_table(&listRec)

	mem.size = 0
	mem.skiplist = CreateSkipList()

}
