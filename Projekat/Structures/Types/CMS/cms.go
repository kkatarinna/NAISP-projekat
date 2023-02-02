package cms

import (
	"bytes"
	"encoding/binary"
)

type Cms struct {
	hash []HashWithSeed
	arr  [][]uint64
	m    uint64
	k    uint64
}

func NewCms(precision float64, assurance float64) *Cms {
	m := CalculateM(precision)
	k := CalculateK(assurance)
	hash := CreateHashFunctions(k)
	arr := make([][]uint64, k)
	for i := range arr {
		arr[i] = make([]uint64, m)
	}

	cms := &Cms{hash: hash, arr: arr, m: uint64(m), k: uint64(k)}

	return cms
}

func (bf Cms) Add(data string) {

	for i, fn := range bf.hash {

		data := []byte(data)
		bf.arr[i][(fn.Hash(data) % 10)] += 1

	}
}

func (bf Cms) Check(data string) int {

	bin := []byte(data)

	var max int = int(bf.arr[0][(bf.hash[0].Hash(bin) % 10)])

	for i := 1; i < len(bf.hash); i++ {

		var br int = int(bf.arr[i][(bf.hash[i].Hash(bin) % 10)])

		if br < max {
			max = br
		}

	}

	return max
}

func (cms *Cms) Encode() []byte {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, cms.m)
	binary.Write(&buffer, binary.LittleEndian, cms.k)

	for i := 0; uint64(i) < cms.k; i++ {

		binary.Write(&buffer, binary.LittleEndian, cms.arr[i])

	}

	for i := 0; uint64(i) < cms.k; i++ {

		binary.Write(&buffer, binary.LittleEndian, cms.hash[i].Seed)

	}

	return buffer.Bytes()
}

func (Cms) Decode(buffer bytes.Buffer) *Cms {

	cms := NewCms(0.1, 0.1)
	seed := make([]byte, 32)

	err := binary.Read(&buffer, binary.LittleEndian, &cms.m)
	if err != nil {
		return nil
	}

	err = binary.Read(&buffer, binary.LittleEndian, &cms.k)
	if err != nil {
		return nil
	}

	arr := make([][]uint64, cms.k)
	for i := range arr {
		arr[i] = make([]uint64, cms.m)
	}

	for i := 0; uint64(i) < cms.k; i++ {

		err = binary.Read(&buffer, binary.LittleEndian, &arr[i])
		if err != nil {
			return nil
		}

	}

	cms.arr = arr

	h := make([]HashWithSeed, cms.k)

	for i := 0; uint64(i) < cms.k; i++ {

		err = binary.Read(&buffer, binary.LittleEndian, &seed)
		if err != nil {
			return nil
		}

		hfn := HashWithSeed{Seed: seed}

		seed = make([]byte, 32)

		h[i] = hfn

	}

	cms.hash = h

	return cms
}
