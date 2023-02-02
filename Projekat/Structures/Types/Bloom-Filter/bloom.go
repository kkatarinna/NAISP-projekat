package bloom

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
)

type Bloom struct {
	hash []HashWithSeed
	arr  []byte
	m    uint64
	k    uint64
}

func NewBloom(elem uint64, rate float64) Bloom {
	m := uint64(CalculateM(int(elem), rate))
	k := uint64(CalculateK(int(elem), uint(m)))
	fmt.Println(k)
	arr := make([]byte, m)
	hash := CreateHashFunctions(uint(k))

	for i := 0; i < int(m); i++ {

		arr[i] = 0

	}

	bf := Bloom{hash: hash, arr: arr, m: m, k: k}

	return bf
}

func (bf Bloom) Add(data string) {

	for _, fn := range bf.hash {

		data := []byte(data)
		bf.arr[(fn.Hash(data) % uint64(bf.m))] = 1
	}

}

func (bf Bloom) Check(data string) bool {

	for _, fn := range bf.hash {

		data := []byte(data)
		if bf.arr[(fn.Hash(data)%uint64(bf.m))] == 0 {
			return false
		}
	}
	return true
}

func (bf *Bloom) Encode() []byte {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, bf.m)
	binary.Write(&buffer, binary.LittleEndian, bf.k)
	binary.Write(&buffer, binary.LittleEndian, bf.arr)

	for i := 0; uint64(i) < bf.k; i++ {

		binary.Write(&buffer, binary.LittleEndian, bf.hash[i].Seed)

	}

	return buffer.Bytes()
}

func (Bloom) Decode(fr *bufio.Reader) *Bloom {

	bf := NewBloom(0, 0)
	seed := make([]byte, 32)

	err := binary.Read(fr, binary.LittleEndian, &bf.m)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &bf.k)
	if err != nil {
		return nil
	}

	arr := make([]byte, bf.m)

	err = binary.Read(fr, binary.LittleEndian, &arr)
	if err != nil {
		return nil
	}

	bf.arr = arr

	h := make([]HashWithSeed, bf.k)

	for i := 0; uint64(i) < bf.k; i++ {

		err = binary.Read(fr, binary.LittleEndian, &seed)
		if err != nil {
			return nil
		}

		hfn := HashWithSeed{Seed: seed}

		seed = make([]byte, 32)

		h[i] = hfn

	}

	bf.hash = h

	return &bf
}

func (bf *Bloom) Print() {
	fmt.Println(bf.m)
	fmt.Println(bf.k)
	fmt.Println(bf.arr)

	for i := 0; uint64(i) < bf.k; i++ {

		fmt.Println(bf.hash[i].Seed)
		fmt.Println(len(bf.hash[i].Seed))

	}

}

func Get_bloom(fr *bufio.Reader) *Bloom {

	bl := (Bloom).Decode(Bloom{}, fr)

	return bl

}
