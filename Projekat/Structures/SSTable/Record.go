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

// 	CRC_SIZE       = T_SIZE + C_SIZE
// 	TOMBSTONE_SIZE = CRC_SIZE + 1
// 	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
// 	VALUE_SIZE     = KEY_SIZE + T_SIZE
// )

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Record struct {
	crc       uint32
	timestamp uint64
	tombstone bool
	keysize   uint64
	valuesize uint64
	Key       string
	Value     []byte
}

func NewRecord(key string, value []byte, tombstone bool, timestamp uint64) *Record {

	var keyb bytes.Buffer
	binary.Write(&keyb, binary.LittleEndian, []byte(key))

	crc := CRC32(keyb.Bytes())

	r := &Record{crc: crc, timestamp: uint64(time.Now().Unix()), tombstone: false, keysize: uint64(keyb.Len()), valuesize: uint64(len(value)), Key: key, Value: value}

	return r
}

func (r *Record) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, r.crc)
	binary.Write(&buffer, binary.LittleEndian, r.timestamp)
	binary.Write(&buffer, binary.LittleEndian, r.tombstone)
	binary.Write(&buffer, binary.LittleEndian, r.keysize)
	binary.Write(&buffer, binary.LittleEndian, r.valuesize)
	binary.Write(&buffer, binary.LittleEndian, []byte(r.Key))
	binary.Write(&buffer, binary.LittleEndian, r.Value)

	return &buffer
}

func Decode(fr *bufio.Reader) *Record {

	r := NewRecord("", make([]byte, 0), false, 0)

	err := binary.Read(fr, binary.LittleEndian, &r.crc)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.timestamp)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.tombstone)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.keysize)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &r.valuesize)
	if err != nil {
		return nil
	}

	key := make([]byte, r.keysize)
	err = binary.Read(fr, binary.LittleEndian, &key)
	if err != nil {
		return nil
	}

	r.Key = string(key[:])

	r.Value = make([]byte, r.valuesize)
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
