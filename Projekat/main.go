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

func HllMeni(config *Config, tokenbucket *TokenBucket, mem *Memtable, cache *Cache) {
	for {
		fmt.Println("1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("x. exit")
		fmt.Printf("Izaberite jednu opciju >>")
		var input string
		fmt.Scanln(&input)

		if input == "x" {
			return
		}
		if !CheckTokenBucket(config, tokenbucket) {
			fmt.Println("\nPrekoracen je broj zahteva u vremenskom intervalu.")
			continue
		}
		if input == "1" {
			successfulPut := PutHll(config, mem)
			if successfulPut {
				fmt.Println("\nPodatak je uspesno unet.")
			} else {
				fmt.Println("\nPodatak nije unet, doslo je do greske.")
			}

		} else if input == "2" {
			successfulGet, value := GetHll(mem, cache)
			if successfulGet {
				fmt.Println("broj podataka: ", value)
			} else {
				fmt.Println("\nPodatak ne postoji")
			}
		} else if input == "3" {
			successfulDelete := DeleteHll(config, mem)
			if successfulDelete {
				fmt.Println("\nPodatak je uspesno obrisan.")
			} else {
				fmt.Println("\nPodatak nije obrisan, doslo je do greske.")
			}
		} else {
			fmt.Println("Nije ispravan unos")
		}
	}
}

func CMSMeni(config *Config, tokenbucket *TokenBucket, mem *Memtable, cache *Cache) {
	for {
		mem.Print()
		fmt.Println("1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("x. exit")
		fmt.Printf("Izaberite jednu opciju >>")
		var input string
		fmt.Scanln(&input)

		if input == "x" {
			return
		}
		if !CheckTokenBucket(config, tokenbucket) {
			fmt.Println("\nPrekoracen je broj zahteva u vremenskom intervalu.")
			continue
		}
		if input == "1" {
			successfulPut := PutCms(config, mem)
			if successfulPut {
				fmt.Println("\nPodatak je uspesno unet.")
			} else {
				fmt.Println("\nPodatak nije unet, doslo je do greske.")
			}

		} else if input == "2" {
			successfulGet, cms := GetCms(mem, cache)
			if successfulGet {
				fmt.Println("podatak se ponavlja: ", GetCmsNum(cms))
			} else {
				fmt.Println("\ncms ne postoji")
			}
		} else if input == "3" {
			successfulDelete := DeleteCms(config, mem)
			if successfulDelete {
				fmt.Println("\nPodatak je uspesno obrisan.")
			} else {
				fmt.Println("\nPodatak nije obrisan, doslo je do greske.")
			}
		} else {
			fmt.Println("Nije ispravan unos")
		}
	}
}

func main() {
	os.MkdirAll(MAIN_DIR_FILES+"/LVL1", os.ModePerm)
	os.MkdirAll(MAIN_DIR_FOLDERS+"/LVL1", os.ModePerm)
	os.MkdirAll("./Data/wal", os.ModePerm)
	config := setConfig()

	fmt.Println(config.SSTable)
	timestamp := time.Now().Unix()
	tokenbucket := &TokenBucket{Time: timestamp, Tokens: config.TokensNumber}

	cache := NewCache(10)
	mem := NewMemPar(config)

	for {
		mem.Print()
		fmt.Println("\n1. PUT")
		fmt.Println("2. GET")
		fmt.Println("3. DELETE")
		fmt.Println("4. MERGE")
		fmt.Println("5. Hll")
		fmt.Println("6. CMS")
		fmt.Println("7. RANGE SCAN")
		fmt.Println("8. LIST")
		fmt.Println("x. exit")
		fmt.Printf("Izaberite jednu opciju >>")
		var input string
		fmt.Scanln(&input)

		if input == "x" {
			return
		}
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
			successfulGet, value := Get(mem, cache)
			if successfulGet {
				fmt.Println("Podatak: ", string(value))
			} else {
				fmt.Println("\nPodatak ne postoji")
			}

		} else if input == "3" {
			successfulDelete := Delete(config, mem)
			if successfulDelete {
				fmt.Println("\nPodatak je uspesno obrisan.")
			} else {
				fmt.Println("\nPodatak nije obrisan, doslo je do greske.")
			}
		} else if input == "4" {
			(SSTable).MergeInit(SSTable{})
			(SSTableFile).MergeInit(SSTableFile{})
		} else if input == "5" {
			HllMeni(config, tokenbucket, mem, cache)
		} else if input == "6" {
			CMSMeni(config, tokenbucket, mem, cache)
		} else if input == "7" {
			Scan(mem)
		} else if input == "8" {
			List(mem)
		} else {
			fmt.Println("Nije ispravan unos")
		}
	}

	// r1 := NewRecord("113_r", []byte("1234"), false, 0)
	// r2 := NewRecord("22_r", []byte("1234"), false, 0)
	// r3 := NewRecord("3_r", []byte("1"), false, 0)
	// // r4 := NewRecord("6_r", []byte("123"), false, 0)
	// // r5 := NewRecord("7_r", []byte("1234"), false, 0)
	// // r6 := NewRecord("8_r", []byte("1234"), false, 0)

	// list := make([]*Record, 0)

	// list = append(list, r1)
	// list = append(list, r2)
	// list = append(list, r3)
	// // list = append(list, r4)
	// // list = append(list, r5)
	// // list = append(list, r6)

	// sst := NewSSTable()

	// sst.Write_table(&list)

	// (SSTable).MergeInit(SSTable{})

	// (SSTable).Find_record(SSTable{}, "2_r")

	// fmt.Println((SSTable).Find_record(SSTable{}, "8_r"))

	// lista := (SSTable).List(SSTable{}, "1", &list)

	// for _, elem := range *lista {

	// 	fmt.Println(elem.Key)
	// }

}
