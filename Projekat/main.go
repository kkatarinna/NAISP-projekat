package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	. "projekat/Structures"
	"time"

	. "projekat/Structures/SSTable"

	// . "projekat/Structures/Types/Bloom-Filter"

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
	os.MkdirAll(MAIN_DIR_FILES+"/LVL1", os.ModePerm)
	os.MkdirAll(MAIN_DIR_FOLDERS+"/LVL1", os.ModePerm)
	config := setConfig()

	timestamp := time.Now().Unix()
	tokenbucket := &TokenBucket{Time: timestamp, Tokens: config.TokensNumber}

	mem := NewMem()

	for {
		fmt.Println("\n1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("x. exit")
		fmt.Printf("Izaberite jednu opciju >>")
		var input string
		fmt.Scanln(&input)
		if !CheckTokenBucket(config, tokenbucket) {
			fmt.Println("\nPrekoracen je broj zahteva u vremenskom intervalu.")
			continue
		}
		if input == "1" {
			successfulPut := Put(config, mem)
			if successfulPut {
				fmt.Println("\nPodatak je uspesno unet.")
			} else {
				fmt.Println("\nPodatak nije unet, doslo je do greske.")
			}

		} else if input == "2" {
			fmt.Println("GET")

		} else if input == "3" {
			successfulDelete := Delete(config, mem)
			if successfulDelete {
				fmt.Println("\nPodatak je uspesno obrisan.")
			} else {
				fmt.Println("\nPodatak nije obrisan, doslo je do greske.")
			}

		} else if input == "x" {
			return
		} else {
			fmt.Println("Nije ispravan unos")
		}
	}

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

	// r := NewRecord("1", []byte("1111"), false, 0)
	// r2 := NewRecord("10", []byte("lala"), true, 0)
	// r3 := NewRecord("12", []byte("lala"), false, 0)

	// list := make([]*Record, 0)

	// list = append(list, r)
	// list = append(list, r2)
	// list = append(list, r3)

	// sst := NewSSTableFile()

	// sst.Write_table(&list)

	// (SSTableFile).MergeInit(SSTableFile{})

	//Find_record_Files("1")

	// hll := (HLL).Decode(HLL{}, &rez.Value)

	// fmt.Println(hll.Estimate())

	// rez := mem.Find("lak")

	// fmt.Println(rez)

}
