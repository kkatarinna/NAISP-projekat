package structures

import (
	"fmt"
	"log"
	. "projekat/Structures/SSTable"
	"time"
)

type Config struct {
	WalSize           uint64 `yaml:"wal_size"`
	Trashold          uint64 `yaml:"trashold"`
	MemtableSize      uint64 `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
	SegmentSize       int    `yaml:"segment_size"`
	TimeInterval      int64  `yaml:"time_interval"`
	TokensNumber      int64  `yaml:"tokens_number"`
	SSTable           string `yaml:"ssTable"`
}

type Memtable struct {
	BTree        *BTree
	Skiplist     *SkipList
	trashold     uint64
	size         uint64
	memtableSize uint64
	walSize      uint64
	ssTable      string
}

func (mem *Memtable) GetSSTable() (bool, *SSTable, *SSTableFile) {
	if mem.ssTable == "file" {
		return true, nil, &SSTableFile{}
	} else {
		return false, &SSTable{}, nil
	}
}

func NewMemPar(c *Config) *Memtable {
	empty, err := IsWalEmpty()
	if err != nil {
		log.Fatal(err)
	}
	//sta ce se desiti ako nije btree da li ce overwrite BTree mem
	mem := &Memtable{Skiplist: nil, BTree: CreateBTree(3), memtableSize: c.MemtableSize, walSize: c.WalSize, trashold: c.Trashold, ssTable: c.SSTable, size: 0}

	if c.MemtableStructure != "btree" {
		mem = &Memtable{Skiplist: CreateSkipList(), BTree: nil, memtableSize: c.MemtableSize, walSize: c.WalSize, trashold: c.Trashold, ssTable: c.SSTable, size: 0}
	}

	if empty {
		return mem
	}

	data, err := ReadAllWal()
	if err != nil {
		log.Fatal(err)
	}
	mem.ReconstructWal(data)

	return mem
}

func NewMem() *Memtable {
	empty, err := IsWalEmpty()
	if err != nil {
		log.Fatal(err)
	}

	mem := &Memtable{BTree: nil, Skiplist: CreateSkipList(), memtableSize: 10, walSize: 10, trashold: 30, size: 0}

	if empty {
		return mem
	}
	data, err := ReadAllWal()
	if err != nil {
		log.Fatal(err)
	}
	mem.ReconstructWal(data)

	return mem
}

func (mem *Memtable) ReconstructWal(data []Record) {
	for _, rec := range data {
		success := false
		if mem.Skiplist == nil {
			success = mem.BTree.AddRecord(mem.BTree, rec)
		} else {
			success = mem.Skiplist.AddRecord(rec)
		}
		if !success {
			panic("error occured")
		}
	}
}

func (mem *Memtable) Insert(key string, value []byte) bool {

	//ako se koristi bTree
	if mem.Skiplist == nil {
		_, _, node2, _ := mem.BTree.Find(key)
		var data *Data
		for _, n := range node2.datas {
			if n.key == key {
				data = &n
			}
		}

		if data != nil {

			data.tombstone = false
			data.value = value
			data.timestamp = uint64(time.Now().Unix())
			data.tombstone = false

		} else {
			mem.BTree.Add(mem.BTree, key, value)
			mem.size++
		}

		if float64(mem.size) >= float64((mem.memtableSize*mem.trashold)/100.0) {
			mem.Flush()
		}

		return true

		//ako se koristi BTree
	} else if mem.BTree == nil {
		node := mem.Skiplist.find(key)

		if node != nil {

			node.tombstone = false
			node.value = value
			node.timestamp = uint64(time.Now().Unix())
			node.tombstone = false

		} else {
			mem.Skiplist.Add(key, value)
			mem.size++
		}

		if float64(mem.size) >= float64((mem.memtableSize*mem.trashold)/100.0) {
			mem.Flush()
		}

		return true
	}

	return false
}

func (mem *Memtable) Find(key string) ([]byte, bool) {

	//ako se koristi skip list
	if mem.BTree == nil {
		node := mem.Skiplist.find(key)
		if node != nil {

			return node.value, node.tombstone
		}
		return nil, false
	} else if mem.Skiplist == nil {
		found, _, node, _ := mem.BTree.Find(key)
		if found {
			for _, data := range node.datas {
				if data.key == key {
					return data.value, data.tombstone
				}
			}
		}
		return nil, false

	}
	return nil, false

}

func (mem *Memtable) Delete(key string) {

	//ako se koristi skip list
	if mem.BTree == nil {
		node := mem.Skiplist.find(key)

		if node != nil {

			if !node.tombstone {
				node.tombstone = true
			}
		} else {
			mem.Skiplist.Add(key, make([]byte, 0))
			mem.Skiplist.logicDelete(key)
			mem.size++
		}
		//ako se koristi BTree
	} else if mem.Skiplist == nil {
		_, _, node, _ := mem.BTree.Find(key)

		var data *Data
		for _, n := range node.datas {
			if n.key == key {
				data = &n
			}
		}
		if data != nil {
			if !data.tombstone {
				data.tombstone = true
			}
		} else {
			mem.BTree.Add(mem.BTree, key, make([]byte, 0))
			mem.BTree.LogicDelete(key)
			mem.size++
		}
	}
	if float64(mem.size) >= float64((mem.memtableSize*mem.trashold)/100.0) {
		mem.Flush()
	}

}

func (mem *Memtable) Flush() {

	//ako se koristi skip list
	if mem.BTree == nil {

		fmt.Println(mem.size)
		listNode := mem.Skiplist.GetAll()

		listRec := make([]*Record, 0)

		for _, element := range *listNode {

			rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
			listRec = append(listRec, rec)

		}

		if mem.ssTable == "file" {

			NewSSTableFile().Write_table(&listRec)

		} else {

			NewSSTable().Write_table(&listRec)
		}

		mem.size = 0
		mem.Skiplist = CreateSkipList()

		//ako koristi BTree
	} else if mem.Skiplist == nil {

		fmt.Println(mem.size)
		listNode := mem.BTree.GetAll()

		listRec := make([]*Record, 0)

		for _, element := range *listNode {

			rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
			listRec = append(listRec, rec)

		}

		if mem.ssTable == "file" {

			NewSSTableFile().Write_table(&listRec)

		} else {

			NewSSTable().Write_table(&listRec)
		}

		mem.size = 0
		mem.BTree = CreateBTree(3)
	}
	err := DeleteWal()
	if err != nil {
		log.Fatal(err)
	}

}

func (mem *Memtable) Print() {
	if mem.Skiplist == nil {
		mem.BTree.PrintBTreeWidth()
	} else {
		mem.Skiplist.Print()
	}
}
