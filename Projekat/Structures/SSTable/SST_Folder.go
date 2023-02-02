package sstable

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	. "projekat/Structures/Types/Bloom-Filter"
	"strconv"
)

const MAIN_DIR_FOLDERS = "./Data/SSTable_Data/SST_Folders"

type SSTable struct {
	dataFile   *BinaryFile
	indexFile  *BinaryFile
	sumFile    *BinaryFile
	filterFile *BinaryFile
	metaFile   *BinaryFile
	TOCPath    string
}

func NewSSTable() *SSTable {

	files, err := ioutil.ReadDir(MAIN_DIR_FOLDERS)

	if err != nil {
		fmt.Print(err)
	}

	i := len(files) + 1

	dir := MAIN_DIR_FOLDERS + "/GEN-" + strconv.Itoa(i)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTable{}

	str := dir + "/usertable-" + strconv.Itoa(i) + "-"

	sst.dataFile = newBinaryFile(str + "Data.db")

	sst.indexFile = newBinaryFile(str + "Index.db")

	sst.filterFile = newBinaryFile(str + "Filter.db")

	sst.metaFile = newBinaryFile(str + "Meta.db")

	sst.sumFile = newBinaryFile(str + "Summary.db")

	sst.TOCPath = str + "TOC.txt"

	return sst

}

func getSSTable(index int) *SSTable {

	files, _ := ioutil.ReadDir(MAIN_DIR_FOLDERS)
	// fmt.Println(len(files))
	i := len(files)

	if index < 0 || index > i {
		return nil
	}

	dir := MAIN_DIR_FOLDERS + "/GEN-" + strconv.Itoa(index)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTable{}

	str := dir + "/usertable-" + strconv.Itoa(index) + "-"

	sst.dataFile = newBinaryFile(str + "Data.db")

	sst.indexFile = newBinaryFile(str + "Index.db")

	sst.filterFile = newBinaryFile(str + "Filter.db")

	sst.metaFile = newBinaryFile(str + "Meta.db")

	sst.sumFile = newBinaryFile(str + "Summary.db")

	sst.TOCPath = str + "TOC.txt"

	return sst

}

func (sst *SSTable) Write_table(list *[]*Record) {

	bloom := NewBloom(uint64(len(*list)), 0.1)

	file, err := os.Create(sst.dataFile.Filename)
	if err != nil {
		fmt.Println("err data file")
	}
	defer file.Close()

	fw := bufio.NewWriter(file)

	offset := uint64(0)

	index_list := make([]*Index, 0)

	for _, element := range *list {

		bloom.Add(element.Key)

		size := fw.Available()
		sst.dataFile.write_record(element, fw)
		size_after := fw.Available()

		index := newIndex(element.keysize, element.Key, offset)
		index_list = append(index_list, index)

		offset = uint64(size-size_after) + offset

		fw.Flush()

	}

	sst.write_bloom(&bloom)
	sst.write_index(&index_list)

}

func (sst *SSTable) write_index(list *[]*Index) {

	file, err := os.Create(sst.indexFile.Filename)
	if err != nil {
		fmt.Println("err data file")
	}
	defer file.Close()

	fw := bufio.NewWriter(file)

	index_list := make([]*Index, 0)
	offset := uint64(0)

	sum := newSummary()

	sum.minValSize = (*list)[0].keysize
	sum.minVal = (*list)[0].key
	sum.maxValSize = (*list)[len(*list)-1].keysize
	sum.maxVal = (*list)[len(*list)-1].key

	for i, element := range *list {

		size := fw.Available()
		sst.indexFile.write_index(element, fw)
		size_after := fw.Available()

		if i%int(sum.compression) == 0 {

			index := newIndex(element.keysize, element.key, offset)
			index_list = append(index_list, index)

		}

		offset = uint64(size-size_after) + offset

		fw.Flush()

	}

	sst.write_summary(sum, &index_list)

}

func (sst *SSTable) write_summary(sum *Summary, list *[]*Index) {

	file, err := os.Create(sst.sumFile.Filename)
	if err != nil {
		fmt.Println("err data file")
	}
	defer file.Close()

	fw := bufio.NewWriter(file)

	sst.sumFile.write_sum(sum, fw)

	for _, element := range *list {

		sst.sumFile.write_index(element, fw)

	}

	fw.Flush()

	sst.write_TOC()

}

func (sst *SSTable) write_TOC() {

	file, err := os.Create(sst.TOCPath)
	if err != nil {
		fmt.Println("err data file")
	}
	defer file.Close()

	_, err2 := file.WriteString(sst.dataFile.Filename + "\n")

	if err2 != nil {
		log.Fatal(err2)
	}

	_, err2 = file.WriteString(sst.indexFile.Filename + "\n")

	if err2 != nil {
		log.Fatal(err2)
	}

	_, err2 = file.WriteString(sst.sumFile.Filename + "\n")

	if err2 != nil {
		log.Fatal(err2)
	}

	_, err2 = file.WriteString(sst.filterFile.Filename + "\n")

	if err2 != nil {
		log.Fatal(err2)
	}

}

func (sst *SSTable) write_bloom(bloom *Bloom) {

	file, err := os.Create(sst.filterFile.Filename)
	if err != nil {
		fmt.Println("NEMA")
	}
	defer file.Close()

	fw := bufio.NewWriter(file)

	sst.filterFile.write_bloom(bloom, fw)

	fw.Flush()

}

func Find_record_Folders(key string) *Record {

	files, _ := ioutil.ReadDir(MAIN_DIR_FOLDERS)
	// fmt.Println(len(files))
	i := len(files)

	if i == 0 {
		return nil
	}

	for ; i > 0; i-- {

		ss := getSSTable(i)

		bloom := ss.filterFile.read_bloom()

		if !bloom.Check(key) {
			continue
		}

		offset_ind := findOffSum(key, ss.sumFile, 0)

		if offset_ind == nil {
			continue
		}

		rec_ind := findOffInd(key, ss.indexFile, uint64(offset_ind.offset))

		if rec_ind == nil {
			continue
		}

		file, _ := os.Open(ss.dataFile.Filename)
		file.Seek(int64(rec_ind.offset), 0)

		fr := bufio.NewReader(file)

		record := Decode(fr)

		fmt.Println(record)

		return record

	}

	return nil
}
