package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
)

type Index struct {
	keysize uint64
	key     string
	offset  uint64
}

func newIndex(keysize uint64, key string, offset uint64) *Index {

	i := &Index{keysize: keysize, key: key, offset: offset}

	return i
}

// serijalizacija
func (i *Index) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, i.keysize)
	binary.Write(&buffer, binary.LittleEndian, []byte(i.key))
	binary.Write(&buffer, binary.LittleEndian, i.offset)

	return &buffer
}

// deserijalizacija
func (Index) Decode(fr *bufio.Reader) *Index {

	i := newIndex(0, "", 0)

	err := binary.Read(fr, binary.LittleEndian, &i.keysize)
	if err != nil {
		return nil
	}

	key := make([]byte, i.keysize)
	err = binary.Read(fr, binary.LittleEndian, key)
	if err != nil {
		return nil
	}

	i.key = string(key[:])

	err = binary.Read(fr, binary.LittleEndian, &i.offset)
	if err != nil {
		return nil
	}

	return i
}

func get_indexes(bf *BinaryFile) *[]Index {

	indexes := make([]Index, 0)

	file, _ := os.Open(bf.Filename)

	fr := bufio.NewReader(file)

	for {

		r := (Index).Decode(Index{}, fr)

		if r == nil {
			break
		}
		indexes = append(indexes, *r)

	}

	return &indexes

}

// pronalazak kljuca i njegovog offseta
func findOffInd(key string, bf *BinaryFile, offset uint64, offset_next uint64) *Index {

	var buff uint64
	file, _ := os.Open(bf.Filename)
	file.Seek(int64(offset), 0)

	fr := bufio.NewReader(file)

	for {

		if offset_next != 0 {

			if buff >= offset_next {
				break
			}

		}

		i := (Index).Decode(Index{}, fr)

		bytess := i.Encode()
		buff += uint64(bytess.Len())

		if i == nil {
			break
		}

		if i.key == key {
			return i
		}

	}

	return nil

}
