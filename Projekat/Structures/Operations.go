package structures

import (
	"fmt"
	. "projekat/Structures/SSTable"
	. "projekat/Structures/Types"
)

func Put(config *Config, mem *Memtable) bool {
	for {
		fmt.Printf("Unesite kljuc >>")
		var key string
		fmt.Scanln(&key)
		key += "_r"

		fmt.Printf("Unesite vrednost >>")
		var value string
		fmt.Scanln(&value)
		fmt.Println("podatak: ", []byte(value))

		successfulWalAppend := AppendRecordWal(config, false, key, []byte(value))
		if !successfulWalAppend {
			return false
		}
		successfulMemInsert := mem.Insert(key, []byte(value))
		if !successfulMemInsert {
			return false
		}
		return true
	}
}

func PutHll(config *Config, mem *Memtable) bool {
	for {
		fmt.Printf("Unesite kljuc >>")
		var key string
		fmt.Scanln(&key)

		fmt.Printf("Unesite vrednost >>")
		var value string
		fmt.Scanln(&value)
		fmt.Println("podatak: ", value)

		

		successfulWalAppend := AppendRecordWal(config, false, key, []byte(value))
		if !successfulWalAppend {
			return false
		}
		successfulMemInsert := mem.Insert(key, []byte(value))
		if !successfulMemInsert {
			return false
		}
		return true
	}
}

func Delete(config *Config, mem *Memtable) bool {
	for {
		fmt.Printf("Unesite kljuc >>")
		var key string
		fmt.Scanln(&key)
		key += "_r"

		successfulWalDelete := AppendRecordWal(config, true, key, []byte("deleted"))
		if !successfulWalDelete {
			return false
		}
		mem.Delete(key)
		return true
	}
}

func Get(mem *Memtable, cache *Cache) (bool, []byte) {

	fmt.Printf("Unesite kljuc >>")
	var key string
	fmt.Scanln(&key)
	key += "_r"

	valmem, tomb := mem.Find(key)
	if valmem != nil {
		if tomb {
			return false, nil
		}
		cache.Set(key, valmem)
		return true, valmem
	}
	valcash, _ := cache.Get(key)
	if valcash != nil {
		cache.Set(key, valcash)
		return true, valcash
	}

	var rec *Record

	if mem.ssTable == "file" {
		rec = (SSTableFile).Find_record(SSTableFile{}, key)
		//ubaciti u cache posle fajla ovde...
	} else {
		rec = (SSTable).Find_record(SSTable{}, key)
		//ubaciti u cache posle fajla ovde...
	}

	if rec != nil {
		return true, rec.Value
	}

	return false, nil
}
