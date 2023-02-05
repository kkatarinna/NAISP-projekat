package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"
)

// const (
// 	T_SIZE = 8
// 	C_SIZE = 4

// 	Crc_SIZE       = T_SIZE + C_SIZE
// 	Tombstone_SIZE = Crc_SIZE + 1
// 	KEY_SIZE       = Tombstone_SIZE + T_SIZE
// 	VALUE_SIZE     = KEY_SIZE + T_SIZE
// )

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Record struct {
	Crc       uint32
	Timestamp uint64
	Tombstone bool
	Keysize   uint64
	Valuesize uint64
	Key       string
	Value     []byte
}

func NewRecord(key string, value []byte, Tombstone bool, Timestamp uint64) *Record {

	var keyb bytes.Buffer
	binary.Write(&keyb, binary.LittleEndian, []byte(key))

	Crc := CRC32(keyb.Bytes())

	r := &Record{Crc: Crc, Timestamp: uint64(time.Now().Unix()), Tombstone: Tombstone, Keysize: uint64(keyb.Len()), Valuesize: uint64(len(value)), Key: key, Value: value}

	return r
}

// ser
func (r *Record) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, r.Crc)
	binary.Write(&buffer, binary.LittleEndian, r.Timestamp)
	binary.Write(&buffer, binary.LittleEndian, r.Tombstone)
	binary.Write(&buffer, binary.LittleEndian, r.Keysize)
	binary.Write(&buffer, binary.LittleEndian, r.Valuesize)
	binary.Write(&buffer, binary.LittleEndian, []byte(r.Key))
	binary.Write(&buffer, binary.LittleEndian, r.Value)

	return &buffer
}

// deser
func Decode(fr *bufio.Reader) *Record {

	r := NewRecord("", make([]byte, 0), false, 0)

	err := binary.Read(fr, binary.LittleEndian, &r.Crc)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.Timestamp)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.Tombstone)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.Keysize)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.Valuesize)
	if err != nil {
		return nil
	}

	key := make([]byte, r.Keysize)
	err = binary.Read(fr, binary.LittleEndian, &key)
	if err != nil {
		return nil
	}

	r.Key = string(key[:])

	r.Value = make([]byte, r.Valuesize)
	err = binary.Read(fr, binary.LittleEndian, &r.Value)
	if err != nil {
		return nil
	}

	return r
}

func get_records(bf *BinaryFile) *[]Record {

	records := make([]Record, 0)

	file, _ := os.Open(bf.Filename)

	fr := bufio.NewReader(file)

	r := Decode(fr)

	for {

		if r == nil {
			break
		}
		records = append(records, *r)
		r = Decode(fr)

	}

	return &records

}
