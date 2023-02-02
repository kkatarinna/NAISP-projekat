package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	. "projekat/Structures/Types/Bloom-Filter"
	"strconv"
)

const MAIN_DIR_FILES = "./Data/SSTable_Data/SST_Files"

type SSTableFile struct {
	sstFile           *BinaryFile
	dataFile_offset   uint64
	indexFile_offset  uint64
	sumFile_offset    uint64
	filterFile_offset uint64
	metaFile_offset   uint64
}

func NewSSTableFile() *SSTableFile {

	files, err := ioutil.ReadDir(MAIN_DIR_FILES)

	if err != nil {
		fmt.Print(err)
	}

	i := len(files) + 1

	dir := MAIN_DIR_FILES + "/GEN-" + strconv.Itoa(i) + ".db"

	sst := &SSTableFile{}

	sst.sstFile = newBinaryFile(dir)

	sst.dataFile_offset = 0

	sst.indexFile_offset = 0

	sst.sumFile_offset = 0

	sst.filterFile_offset = 0

	sst.metaFile_offset = 0

	return sst

}

func getSSTableFile(index int) *SSTableFile {

	files, _ := ioutil.ReadDir(MAIN_DIR_FILES)
	// fmt.Println(len(files))
	i := len(files)

	if index < 0 || index > i {
		return nil
	}

	dir := MAIN_DIR_FILES + "/GEN-" + strconv.Itoa(index) + ".db"

	sst := &SSTableFile{}

	sst.sstFile = newBinaryFile(dir)

	return sst

}

func (sst *SSTableFile) Write_table(list *[]*Record) {

	bloom := NewBloom(uint64(len(*list)), 0.1)

	file, err := os.Create(sst.sstFile.Filename)
	if err != nil {
		fmt.Println("err data file")
	}

	fw := bufio.NewWriter(file)

	offset := uint64(0)

	index_list := make([]*Index, 0)

	for _, element := range *list {

		bloom.Add(element.Key)

		size := fw.Available()
		sst.sstFile.write_record(element, fw)
		size_after := fw.Available()

		index := newIndex(element.keysize, element.Key, offset)
		index_list = append(index_list, index)

		offset = uint64(size-size_after) + offset

		fw.Flush()

	}
	sst.indexFile_offset = offset

	sst.write_index(&index_list, &offset, fw)
	sst.write_bloom(&bloom, &offset, fw)

	sst.dataFile_offset = offset

	sst.write_offsets(fw)
	fmt.Println(4096 - fw.Available())
	fw.Flush()

	file.Close()

	file, err = os.Open(sst.sstFile.Filename)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	// Print the contents of the file
	fmt.Printf("File contents: %v\n", fileContents)

}

func (sst *SSTableFile) write_index(list *[]*Index, prev_offset *uint64, fw *bufio.Writer) {

	index_list := make([]*Index, 0)
	offset := uint64(0)

	sum := newSummary()

	sum.minValSize = (*list)[0].keysize
	sum.minVal = (*list)[0].key
	sum.maxValSize = (*list)[len(*list)-1].keysize
	sum.maxVal = (*list)[len(*list)-1].key

	for i, element := range *list {

		size := fw.Available()
		sst.sstFile.write_index(element, fw)
		size_after := fw.Available()

		if i%int(sum.compression) == 0 {

			index := newIndex(element.keysize, element.key, offset)
			index_list = append(index_list, index)

		}

		offset = uint64(size-size_after) + offset

		fw.Flush()

	}

	*prev_offset += offset
	sst.sumFile_offset = *prev_offset

	sst.write_summary(sum, &index_list, prev_offset, fw)

}

func (sst *SSTableFile) write_summary(sum *Summary, list *[]*Index, prev_offset *uint64, fw *bufio.Writer) {

	size := fw.Available()
	sst.sstFile.write_sum(sum, fw)

	for _, element := range *list {

		sst.sstFile.write_index(element, fw)

	}
	size_after := fw.Available()
	*prev_offset += uint64(size - size_after)
	sst.filterFile_offset = *prev_offset

	fw.Flush()

}

func (sst *SSTableFile) write_bloom(bloom *Bloom, prev_offset *uint64, fw *bufio.Writer) {

	size := fw.Available()
	sst.sstFile.write_bloom(bloom, fw)
	size_after := fw.Available()

	fw.Flush()

	*prev_offset += uint64(size - size_after)

}

func (sst *SSTableFile) write_offsets(fw *bufio.Writer) {

	binary_data := sst.Encode()
	binary.Write(fw, binary.LittleEndian, binary_data.Bytes())

}

func Find_record_Files(key string) *Record {

	files, _ := ioutil.ReadDir(MAIN_DIR_FILES)
	// fmt.Println(len(files))
	i := len(files)

	if i == 0 {
		return nil
	}

	for ; i > 0; i-- {

		ss := getSSTableFile(i)

		file, _ := os.Open(ss.sstFile.Filename)

		file.Seek(-40, 2)
		fr := bufio.NewReader(file)

		ss = ss.Decode(fr, i)

		file.Seek(int64(ss.filterFile_offset), 0)
		fr = bufio.NewReader(file)
		bloom := Get_bloom(fr)

		if !bloom.Check(key) {
			continue
		}

		offset_ind := findOffSum(key, ss.sstFile, ss.sumFile_offset)

		if offset_ind == nil {
			continue
		}

		rec_ind := findOffInd(key, ss.sstFile, ss.indexFile_offset+offset_ind.offset)

		if rec_ind == nil {
			continue
		}

		file.Seek(0, 0)
		fr = bufio.NewReader(file)

		record := Decode(fr)

		fmt.Println(record)

		return record

	}

	return nil
}

func (sst *SSTableFile) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, sst.dataFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.indexFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.sumFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.filterFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.metaFile_offset)

	return &buffer
}

func (SSTableFile) Decode(fr *bufio.Reader, i int) *SSTableFile {

	sst := getSSTableFile(i)

	err := binary.Read(fr, binary.LittleEndian, &sst.dataFile_offset)
	if err != nil {
		return nil
	}

	err = binary.Read(fr, binary.LittleEndian, &sst.indexFile_offset)
	if err != nil {
		return nil
	}
	err = binary.Read(fr, binary.LittleEndian, &sst.sumFile_offset)
	if err != nil {
		return nil
	}
	err = binary.Read(fr, binary.LittleEndian, &sst.filterFile_offset)
	if err != nil {
		return nil
	}
	err = binary.Read(fr, binary.LittleEndian, &sst.metaFile_offset)
	if err != nil {
		return nil
	}

	return sst
}
