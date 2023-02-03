package structures

import (
	"fmt"
)
func Put(config *Config, mem *Memtable) bool {
	for {
		fmt.Printf("Unesite kljuc >>")
		var key string
		fmt.Scanln(&key)

		fmt.Printf("Unesite vrednost >>")
		var value string
		fmt.Scanln(&value)

		successfulWalAppend := AppendRecordWal(config, false, key, []byte(value))
		if !successfulWalAppend {
			return false
		}
		successfulMemInsert := mem.Insert(key, []byte(value))
		if !successfulMemInsert  {
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

		successfulWalDelete := AppendRecordWal(config, true, key, []byte("deleted"))
		if !successfulWalDelete {
			return false
		}
		mem.Delete(key)
		return true
	}
}

//func Get(key string, mem *Memtable) []byte {
//	
//
//    key1 := sstable.Find_record_Files(key)
//    key2 := sstable.Find_recordFolders(key)
//
//    valmem := mem.Find(key)
//    if valmem != nil {
//        cash.Set(key, valmem)
//        return mem.Find(key)
//    }
//    valcash,_  := cash.Get(key)
//    if valcash != nil {
//        cash.Set(key, valcash)
//        return valcash
//    }
//
//    if key1 != nil {
//        cash.Set(key, key1.Value)
//        return key1.Value
//    }
//
//    if key2 != nil {
//        cash.Set(key, key2.Value)
//        return key2.Value
//    }
//
//    return nil
//}