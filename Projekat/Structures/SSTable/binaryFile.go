package sstable

import (
	"bufio"
	"encoding/binary"
	"os"
	. "projekat/Structures/Types/Bloom-Filter"
)

type BinaryFile struct {
	Filename string
}

func newBinaryFile(file string) *BinaryFile {
	bf := &BinaryFile{Filename: file}
	return bf
}

func (bf *BinaryFile) write_record(rec *Record, fw *bufio.Writer) {

	binary_data := rec.Encode()
	binary.Write(fw, binary.LittleEndian, binary_data.Bytes())

}

func (bf *BinaryFile) write_index(i *Index, fw *bufio.Writer) {

	binary_data := i.Encode()
	binary.Write(fw, binary.LittleEndian, binary_data.Bytes())

}

func (bf *BinaryFile) write_sum(sum *Summary, fw *bufio.Writer) {

	binary_data := sum.Encode()
	binary.Write(fw, binary.LittleEndian, binary_data.Bytes())

}

func (bf *BinaryFile) write_bloom(bl *Bloom, fw *bufio.Writer) {

	binary_data := bl.Encode()
	binary.Write(fw, binary.LittleEndian, binary_data)

}

func (bf *BinaryFile) read_bloom() *Bloom {

	file, _ := os.Open(bf.Filename)

	fr := bufio.NewReader(file)

	bl := Get_bloom(fr)

	return bl

}
