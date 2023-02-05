package structures

import (
	"bytes"
	"fmt"
	. "projekat/Structures/SSTable"
	. "projekat/Structures/Types"
	. "projekat/Structures/Types/CMS"
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

	hll := NewHll(10)
	fmt.Printf("Unesite kljuc >>")
	var key string
	fmt.Scanln(&key)
	key += "_hll"
	var value string

	//unosi niz stringova za hll
	for{
		fmt.Printf("Unesite vrednost (x za kraj)>>")
		fmt.Scanln(&value)
		if value == "x"{
			break
		}
		hll.Put(value)
	}

	//upisivanje serijalizovan niz stringova pod jednim kljucem
	for{
		successfulWalAppend := AppendRecordWal(config, false, key, hll.Encode())
		if !successfulWalAppend {
			return false
		}
		successfulMemInsert := mem.Insert(key, hll.Encode())
		if !successfulMemInsert {
			return false
		}
		return true
	}
}

func PutCms(config *Config, mem *Memtable) bool {

	cms := NewCms(0.1,0.1)
	fmt.Printf("Unesite kljuc >>")
	var key string
	fmt.Scanln(&key)
	key += "_cms"
	var value string

	//unosi niz stringova za cms
	for{
		fmt.Printf("Unesite vrednost (x za kraj)>>")
		fmt.Scanln(&value)
		if value == "x"{
			break
		}
		cms.Add(value)
	}

	//upisivanje serijalizovan niz stringova pod jednim kljucem
	for{
		successfulWalAppend := AppendRecordWal(config, false, key,cms.Encode())
		if !successfulWalAppend {
			return false
		}
		successfulMemInsert := mem.Insert(key, cms.Encode())
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

func DeleteCms(config *Config, mem *Memtable) bool {
	for {
		fmt.Printf("Unesite kljuc >>")
		var key string
		fmt.Scanln(&key)
		key += "_cms"

		successfulWalDelete := AppendRecordWal(config, true, key, []byte("deleted"))
		if !successfulWalDelete {
			return false
		}
		mem.Delete(key)
		return true
	}
}

func DeleteHll(config *Config, mem *Memtable) bool {
	for {
		fmt.Printf("Unesite kljuc >>")
		var key string
		fmt.Scanln(&key)
		key += "_hll"

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
		if rec != nil{
			cache.Set(rec.Key, rec.Value)
		}
	} else {
		rec = (SSTable).Find_record(SSTable{}, key)
		if rec != nil{
			cache.Set(rec.Key, rec.Value)
		}
	}

	if rec != nil {
		return true, rec.Value
	}

	return false, nil
}


func GetHll(mem *Memtable, cache *Cache) (bool, float64) {

	fmt.Printf("Unesite kljuc >>")
	var key string
	fmt.Scanln(&key)
	key += "_hll"

	var buffer bytes.Buffer
	valmem, tomb := mem.Find(key)
	
	//pretraga u memTable
	_,err :=buffer.Write(valmem)
	if valmem != nil {
		if err != nil{
			fmt.Print("error code : 69")
			return false,-1
		}
		if tomb {
			return false, -1
		}
		return true, (HLL).Decode(HLL{},buffer).Estimate()
	}

	//pretraga u cache
	valcash, _ := cache.Get(key)
	_,err =buffer.Write(valcash)
	if valcash != nil {
		if err != nil{
			fmt.Print("error code : 69+1")
			return false,-1
		}
		return true, (HLL).Decode(HLL{},buffer).Estimate()
	}

	//nije pronadjeno ni  u mem ni u cache
	var rec *Record

	//pretraga u fajlovima
	if mem.ssTable == "file" {
		rec = (SSTableFile).Find_record(SSTableFile{}, key)
		if rec != nil{
			cache.Set(rec.Key, rec.Value)
		}
	} else {
		rec = (SSTable).Find_record(SSTable{}, key)
		if rec != nil{
			cache.Set(rec.Key, rec.Value)
		}
	}

	//da li je pronadjeno u fajlovima
	if rec != nil {
		_,err =buffer.Write(rec.Value)
		return true, (HLL).Decode(HLL{},buffer).Estimate()
	}

	return false, -1
}

func GetCms(mem *Memtable, cache *Cache) (bool, *Cms) {

	fmt.Printf("Unesite kljuc >>")
	var key string
	fmt.Scanln(&key)
	key += "_cms"

	var buffer bytes.Buffer
	valmem, tomb := mem.Find(key)
	
	//pretraga za cms
	_,err :=buffer.Write(valmem)
	if valmem != nil {
		if err != nil{
			fmt.Print("error code : 69")
			return false,nil
		}
		if tomb {
			return false, nil
		}
		return true, (Cms).Decode(Cms{},buffer)
	}

	//pretraga u cache
	valcash, _ := cache.Get(key)
	_,err =buffer.Write(valcash)
	if valcash != nil {
		if err != nil{
			fmt.Print("error code : 69+1")
			return false,nil
		}
		return true, (Cms).Decode(Cms{},buffer)
	}

	//nije pronadjeno ni  u mem ni u cache
	var rec *Record

	//pretraga u fajlovima
	if mem.ssTable == "file" {
		rec = (SSTableFile).Find_record(SSTableFile{}, key)
		if rec != nil{
			cache.Set(rec.Key, rec.Value)
		}
	} else {
		rec = (SSTable).Find_record(SSTable{}, key)
		if rec != nil{
			cache.Set(rec.Key, rec.Value)
		}
	}

	//da li je pronadjeno u fajlovima
	if rec != nil {
		_,err =buffer.Write(rec.Value)
		return true, (Cms).Decode(Cms{},buffer)
	}

	return false, nil
}

func GetCmsNum(cms *Cms) int{

	fmt.Printf("Unesite vrednost >>")
	var value string
	fmt.Scanln(&value)

	return cms.Check(value)

}

func Scan(mem *Memtable){

	fmt.Printf("Unesite min >>")
	var min string
	fmt.Scanln(&min)

	fmt.Printf("Unesite max >>")
	var max string
	fmt.Scanln(&max)

	file,ssFile,ss := mem.GetSSTable()
	
	if(!file){
		if(mem.Skiplist != nil){

			listNode := mem.Skiplist.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records:= ssFile.Range(min,max,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 

		}else if(mem.BTree != nil){

			listNode := mem.BTree.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records := ssFile.Range(min,max,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 

		}
	}else{
		if(mem.Skiplist != nil){

			listNode := mem.Skiplist.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records:= ss.Range(min,max,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 

		}else if(mem.BTree != nil){

			listNode := mem.BTree.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records := ss.Range(min,max,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 
		}
	}
	fmt.Print("doslo je do greske")
}

func List(mem *Memtable){

	fmt.Printf("Unesite prefiks >>")
	var value string
	fmt.Scanln(&value)

	file,ssFile,ss := mem.GetSSTable()
	
	if(!file){
		if(mem.Skiplist != nil){

			listNode := mem.Skiplist.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records:= ssFile.List(value,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 

		}else if(mem.BTree != nil){

			listNode := mem.BTree.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records := ssFile.List(value,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 

		}
	}else{
		if(mem.Skiplist != nil){

			listNode := mem.Skiplist.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records:= ss.List(value,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 

		}else if(mem.BTree != nil){

			listNode := mem.BTree.GetAll()
	
			listRec := make([]*Record, 0)
	
			for _, element := range *listNode {
	
				rec := NewRecord(element.key, element.value, element.tombstone, element.timestamp)
				listRec = append(listRec, rec)
	
			}

			records := ss.List(value,&listRec)
			for _,r:= range *records{
				fmt.Println("kljuc: ",r.Key,", vrednost: ",string(r.Value))
			}
			return 
		}
	}
	fmt.Print("doslo je do greske")

}
