package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	. "projekat/Structures"

	// . "projekat/Structures/SSTable"

	// . "projekat/Structures/Types/CMS"

	"gopkg.in/yaml.v2"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func setConfig() *Config {

	var config Config

	configData, err := ioutil.ReadFile("./Data/Configuration/config.yml")
	if err != nil {
		log.Fatal(err)
	}
	yaml.Unmarshal(configData, &config)

	return &config

}

func main() {

	// //append Record
	// AppendRecordWal(false, "key3", []byte("value3"))
	// AppendRecordWal(false, "key2", []byte("value3"))
	// AppendRecordWal(true, "key3", []byte("value3"))

	// //read
	// data, err := ReadAllWal()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(data)

	// for {
	// 	fmt.Println("\n1. PUT")
	// 	fmt.Println("2. GET")
	// 	fmt.Println("3. DELETE")
	// 	fmt.Println("x. exit")
	// 	fmt.Printf("Izaberite jednu opciju >>")
	// 	var input string
	// 	fmt.Scanln(&input)
	// 	if input == "1" {
	// 		fmt.Println("PUT")
	// 	} else if input == "2" {
	// 		fmt.Println("GET")
	// 	} else if input == "3" {
	// 		fmt.Println("DELETE")
	// 	} else if input == "x" {
	// 		return
	// 	} else {
	// 		fmt.Println("Nije ispravan unos")
	// 	}
	// }

	// var bf = NewCms(0.1, 0.1)

	// bf.Add("Jovan")
	// bf.Add("Jovan")
	// bf.Add("Bokicar")
	// bf.Add("Bokicar")
	// bf.Add("Bokicar")

	// fmt.Println(bf.Check("Bokicar"))

	// rez := bf.Encode()

	// var buffer bytes.Buffer

	// binary.Write(&buffer, binary.LittleEndian, rez)

	// cms := (Cms).Decode(Cms{}, buffer)

	// fmt.Println(cms.Check("o"))

	// config := setConfig()

	// mem := NewMem()
	// fmt.Println(mem.Skiplist.Print())

	// mem.Insert("bora", []byte("1111"))
	// mem.Insert("isa", []byte("lala"))
	// mem.Insert("isa", []byte("lala"))
	// mem.Insert("lisa", []byte("lala"))
	// mem.Insert("klisa", []byte("lala"))
	// mem.Delete("boki")
	// mem.Insert("baki", []byte("lalag"))

	// rand.Seed(time.Now().UnixNano())

	// d := NewHll(14)

	// for i := 0; i < 150000; i++ {
	// 	d.Put(randSeq(10))
	// }

	// // d.print()
	// fmt.Println(d.Estimate())

	// fmt.Print("\n\n\n\n")

	// d.emptyCount()

	// r := NewRecord("1", d.Encode(), true, 0)
	// r2 := NewRecord("2", []byte("lala"), true, 0)
	// r3 := NewRecord("5", []byte("lala"), true, 0)

	// list := make([]*Record, 0)

	// list = append(list, r)
	// list = append(list, r2)
	// list = append(list, r3)

	// sst := NewSSTable()

	// sst.Write_table(&list)

	// rez := Find_record_Folders("1")

	// hll := (HLL).Decode(HLL{}, &rez.Value)

	// fmt.Println(hll.Estimate())

	// rez := mem.Find("lak")

	// fmt.Println(rez)

}
