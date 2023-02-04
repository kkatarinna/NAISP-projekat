package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
)

type Summary struct {
	compression uint64
	maxValSize  uint64
	minValSize  uint64
	maxVal      string
	minVal      string
}

func newSummary() *Summary {

	s := &Summary{maxValSize: 0, minValSize: 0, maxVal: "", minVal: "", compression: 2}

	return s
}

func newSummaryPar(maxValSize uint64, minValSize uint64, maxVal string, minVal string) *Summary {

	s := &Summary{maxValSize: maxValSize, minValSize: minValSize, maxVal: maxVal, minVal: minVal, compression: 2}

	return s
}

func (sum *Summary) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, sum.compression)
	binary.Write(&buffer, binary.LittleEndian, sum.maxValSize)
	binary.Write(&buffer, binary.LittleEndian, sum.minValSize)
	binary.Write(&buffer, binary.LittleEndian, []byte(sum.maxVal))
	binary.Write(&buffer, binary.LittleEndian, []byte(sum.minVal))

	return &buffer
}

func (Summary) Decode(fr *bufio.Reader) *Summary {

	sum := newSummary()

	err := binary.Read(fr, binary.LittleEndian, &sum.compression)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &sum.maxValSize)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &sum.minValSize)
	if err != nil {
		return nil
	}

	maxVal := make([]byte, sum.maxValSize)
	err = binary.Read(fr, binary.LittleEndian, maxVal)
	if err != nil {
		return nil
	}
	sum.maxVal = string(maxVal[:])

	minVal := make([]byte, sum.minValSize)
	err = binary.Read(fr, binary.LittleEndian, minVal)
	if err != nil {
		return nil
	}
	sum.minVal = string(minVal[:])

	return sum
}

func get_sum(fr *bufio.Reader) *Summary {

	// file, _ := os.Open(bf.Filename)

	// fr := bufio.NewReader(file)

	r1 := (Summary).Decode(Summary{}, fr)

	return r1

}

func findOffSum(key string, bf *BinaryFile, offset uint64) *Index {

	file, _ := os.Open(bf.Filename)

	file.Seek(int64(offset), 0)

	fr := bufio.NewReader(file)

	h := get_sum(fr)

	min := string(h.minVal[:])
	max := string(h.maxVal[:])

	if min == max {
		if min == key {
			return (Index).Decode(Index{}, fr)
		}
		return nil
	}

	if key < min || key > max {

		return nil

	}

	r := (Index).Decode(Index{}, fr)

	r_next := (Index).Decode(Index{}, fr)

	for {

		if r_next == nil {
			return r
		}

		if key >= (*r).key && key <= (*r_next).key {

			return r

		}

		if r_next.key == max {
			return nil
		}

		r = r_next

		r_next = (Index).Decode(Index{}, fr)

	}

	return nil

}
