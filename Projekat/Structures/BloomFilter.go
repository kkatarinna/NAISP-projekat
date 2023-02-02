package structures

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"crypto/md5"
	"encoding/binary"
	"time"

	"math"
)

//=====================================================
//params.go
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

//====================================================
//hash.go
type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

//=================================================

func mainn() {
	fns := CreateHashFunctions(5)

	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	decoder := gob.NewDecoder(buf)

	for _, fn := range fns {
		data := []byte("hello")
		fmt.Println(fn.Hash(data))
		err := encoder.Encode(fn)
		if err != nil {
			panic(err)
		}
		dfn := &HashWithSeed{}
		err = decoder.Decode(dfn)
		if err != nil {
			panic(err)
		}
		fmt.Println(dfn.Hash(data))
	}

}

//==============================
type BloomFilter struct {
	m   uint64 //velicina za niz bitova (kolko imam 0)
	k   uint64 //broj hesh funkcije
	set []byte //pravi niz gde su 0 i 1
}

//Konstruktor za bloom filter koji se sastavlja na osnovu izracunavanja k i m
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {

	m1 := CalculateM(expectedElements, falsePositiveRate)
	k1 := CalculateK(expectedElements, m1)
	set1 := make([]byte, m1, m1)

	return &BloomFilter{
		m:   uint64(m1),
		k:   uint64(k1),
		set: set1,
	}
}

type BloomInterface interface {
	add(item []byte)
	check(item []byte) bool
}

func (bf BloomFilter) add(item string) {
	digits := []int{} //samo u svrhe lakseg debagovanja i provere (nicemu ne sluzi)
	m := bf.m
	k := bf.k
	set := bf.set

	funkcije := CreateHashFunctions(uint(k))

	for i, fn := range funkcije {
		didget := getIndex(item, i, m, fn)
		digits = append(digits, int(didget))
		set[didget] = 1
	}
}

func (bf BloomFilter) check(item string) bool {
	m := bf.m
	k := bf.k
	set := bf.set
	exists := true
	funkcije := CreateHashFunctions(uint(k))
	for i, fn := range funkcije {
		didget := getIndex(item, i, m, fn)
		if set[didget] == 0 {
			exists = false
		}
	}
	return exists
}

func getIndex(item string, i int, m uint64, fn HashWithSeed) uint64 {
	data := []byte(item)
	hash := fn.Hash(data)
	index := hash % m
	return index
}

// func main() {
// 	fmt.Println("Probanje bloom filtera")
// 	blum := NewBloomFilter(5, 0.052)
// 	blum.add("kata")
// 	blum.add("nidza")
// 	blum.add("deterdzent")
// 	blum.add("steva")
// 	fmt.Println(blum.set)
// 	fmt.Println(blum.check("deterdzent"))
// }
