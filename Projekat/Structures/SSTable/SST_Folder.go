package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	. "projekat/Structures/Types/Bloom-Filter"
	"sort"
	"strconv"
	"strings"
)

const MAIN_DIR_FOLDERS = "./Data/SSTable_Data/SST_Folders"
const LVL1 = 2
const LVL2 = 2
const LVL3 = 2
const MAX_LVL = 4

var lvlMap = map[int]int{
	1: 2,
	2: 2,
	3: 2,
	4: 2,
}

type SSTable struct {
	dataFile   *BinaryFile
	indexFile  *BinaryFile
	sumFile    *BinaryFile
	filterFile *BinaryFile
	metaPath   string
	TOCPath    string
}

func NewSSTable() *SSTable {

	files, err := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL1")

	if err != nil {
		fmt.Print(err)
	}

	i := len(files) + 1

	dir := MAIN_DIR_FOLDERS + "/LVL1" + "/GEN-" + strconv.Itoa(i)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTable{}

	str := dir + "/1usertable-" + strconv.Itoa(i) + "-"

	sst.dataFile = newBinaryFile(str + "Data.db")

	sst.indexFile = newBinaryFile(str + "Index.db")

	sst.filterFile = newBinaryFile(str + "Filter.db")

	sst.sumFile = newBinaryFile(str + "Summary.db")

	sst.metaPath = str + "Meta.txt"

	sst.TOCPath = str + "TOC.txt"

	return sst

}

func GetSSTableParam(lvl int, gen int) *SSTable {

	dir := MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(lvl) + "/GEN-" + strconv.Itoa(gen)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTable{}

	str := dir + "/" + strconv.Itoa(lvl) + "usertable-" + strconv.Itoa(gen) + "-"

	sst.dataFile = newBinaryFile(str + "Data.db")

	sst.indexFile = newBinaryFile(str + "Index.db")

	sst.filterFile = newBinaryFile(str + "Filter.db")

	sst.sumFile = newBinaryFile(str + "Summary.db")

	sst.metaPath = str + "Meta.txt"

	sst.TOCPath = str + "TOC.txt"

	return sst

}

// func getSSTable(index int) *SSTable {

// 	files, _ := ioutil.ReadDir(MAIN_DIR_FOLDERS)
// 	// fmt.Println(len(files))
// 	i := len(files)

// 	if index < 0 || index > i {
// 		return nil
// 	}

// 	dir := MAIN_DIR_FOLDERS + "/GEN-" + strconv.Itoa(index)

// 	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
// 		log.Fatal(err)
// 	}

// 	sst := &SSTable{}

// 	str := dir + "/usertable-" + strconv.Itoa(index) + "-"

// 	sst.dataFile = newBinaryFile(str + "Data.db")

// 	sst.indexFile = newBinaryFile(str + "Index.db")

// 	sst.filterFile = newBinaryFile(str + "Filter.db")

// 	sst.sumFile = newBinaryFile(str + "Summary.db")

// 	sst.metaPath = str + "Meta.db"

// 	sst.TOCPath = str + "TOC.txt"

// 	return sst

// }

func Rename(this_dir int) {

	dir := MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(this_dir)
	files, _ := ioutil.ReadDir(dir)

	for i := 0; i < len(files); i++ {

		strArr := []rune((files)[i].Name())
		gen := string(strArr[4:])
		replace := dir + "/GEN-" + strconv.Itoa(i+1)
		os.Rename(dir+"/"+(files)[i].Name(), replace)
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Data.db", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Data.db")
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Index.db", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Index.db")
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Filter.db", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Filter.db")
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Meta.txt", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Meta.txt")
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Summary.db", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Summary.db")
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-TOC.txt", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-TOC.txt")

	}

}

func (sst *SSTable) Write_table(list *[]*Record) {

	sort.Slice(*list, func(i, j int) bool {
		return (*list)[i].Key < (*list)[j].Key
	})

	bloom := NewBloom(uint64(len(*list)), 0.1)
	merkle_r := CreateMerkleRoot()
	merkle_b := make([][]byte, 0)
	for i := range merkle_b {
		merkle_b[i] = make([]byte, 0)
	}

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
		merkle_b = append(merkle_b, element.Value)

		size := fw.Available()
		sst.dataFile.write_record(element, fw)
		size_after := fw.Available()

		index := newIndex(element.Keysize, element.Key, offset)
		index_list = append(index_list, index)

		offset = uint64(size-size_after) + offset

		fw.Flush()

	}

	sst.write_bloom(&bloom)
	sst.write_index(&index_list)

	merkle_r.FormMerkleTree(sst.metaPath, merkle_b, true)

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

		if i == len(*list) {
			break
		}

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

	element := (*list)[len(*list)-1]
	index := newIndex(element.keysize, element.key, offset)
	index_list = append(index_list, index)

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

func (SSTable) Find_record(key string) *Record {

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		files, _ := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(lvl))
		// fmt.Println(len(files))
		i := len(files)

		for ; i > 0; i-- {

			ss := GetSSTableParam(lvl, i)

			bloom := ss.filterFile.read_bloom()

			if !bloom.Check(key) {
				continue
			}

			offset_ind := findOffSum(key, ss.sumFile, 0)

			if offset_ind == nil {
				continue
			}

			rec_ind := findOffInd(key, ss.indexFile, uint64(offset_ind.offset), 0)

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
	}
	return nil
}

func (SSTable) List(key string, records_mem *[]*Record) *[]*Record {

	lista := make([]*Record, 0)
	readers := make([]*bufio.Reader, 0)
	ssts := make([]*SSTable, 0)
	records_data := make([]*Record, 0)
	var buffer bytes.Buffer

	for _, rec := range *records_mem {

		binary.Write(&buffer, binary.LittleEndian, rec.Encode().Bytes())

	}
	fr := bufio.NewReader(&buffer)
	readers = append(readers, (*bufio.Reader)(fr))

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		os.MkdirAll(MAIN_DIR_FOLDERS+"/LVL"+strconv.Itoa(lvl), os.ModePerm)
		files, _ := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(lvl))

		for _, file := range files {

			strArr := []rune((file).Name())
			gen, _ := strconv.Atoi(string(strArr[4:]))
			ss1 := GetSSTableParam(lvl, gen)
			ssts = append(ssts, ss1)
			// file1, err := os.Open(ss1.dataFile.Filename)
			// if err != nil {
			// 	fmt.Println("NEMA")
			// }
			// defer file1.Close()
			// fr := bufio.NewReader(file1)
			// readers = append(readers, fr)

		}

	}

	for ind, ss := range ssts {

		file, _ := os.Open(ss.sumFile.Filename)

		fr := bufio.NewReader(file)

		h := get_sum(fr)

		min := string(h.minVal[:])

		var offset_ind *Index

		if key < min {
			offset_ind = (Index).Decode(Index{}, fr)
		} else {

			offset_ind = findOffSum(key, ss.sumFile, 0)

			if offset_ind == nil {
				readers = append(readers, nil)
				continue
			}

		}

		file, _ = os.Open(ss.indexFile.Filename)
		file.Seek(int64(offset_ind.offset), 0)

		fr = bufio.NewReader(file)

		var start_index *Index

		start_index = nil

		for {

			i := (Index).Decode(Index{}, fr)

			if i == nil {
				break
			}

			if strings.HasPrefix(i.key, key) {
				start_index = i
				break

			}

		}

		if start_index == nil {
			readers[ind] = nil
			continue
		} else {
			file, _ := os.Open(ss.dataFile.Filename)
			file.Seek(int64(start_index.offset), 0)

			fr = bufio.NewReader(file)
			readers = append(readers, fr)
		}

	}

	for _, reader := range readers {
		if reader == nil {
			records_data = append(records_data, nil)
			continue
		}
		records_data = append(records_data, Decode(reader))
	}

	var r_upis *Record
	var min_ind int

	for {
		r_upis = nil

		for a, record := range records_data {

			if record != nil {
				r_upis = record
				min_ind = a
				break
			}

		}

		if r_upis == nil {
			break
		}

		for i := min_ind + 1; i < len(records_data); i++ {

			if records_data[i] == nil {
				continue
			} else if records_data[i].Key < r_upis.Key {
				r_upis = records_data[i]
				min_ind = i
			} else if records_data[i].Key == r_upis.Key {

				if records_data[i].Timestamp > r_upis.Timestamp {

					r_upis = records_data[i]
					records_data[min_ind] = Decode(readers[min_ind])
					min_ind = i

				} else {
					records_data[i] = Decode(readers[i])

				}

			}

		}

		if !r_upis.Tombstone {
			if strings.HasPrefix(r_upis.Key, key) {
				if !In(r_upis.Key, &lista) {
					lista = append(lista, r_upis)
				}

			} else {
				if min_ind != 0 {

					break

				}
			}
		}

		records_data[min_ind] = Decode(readers[min_ind])

	}
	fmt.Println(lista)

	return &lista

}

func (SSTable) Range(min string, max string, records_mem *[]*Record) *[]*Record {

	lista := make([]*Record, 0)
	readers := make([]*bufio.Reader, 0)
	ssts := make([]*SSTable, 0)
	records_data := make([]*Record, 0)
	var buffer bytes.Buffer

	for _, rec := range *records_mem {

		binary.Write(&buffer, binary.LittleEndian, rec.Encode().Bytes())

	}
	fr := bufio.NewReader(&buffer)
	readers = append(readers, (*bufio.Reader)(fr))

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		os.MkdirAll(MAIN_DIR_FOLDERS+"/LVL"+strconv.Itoa(lvl), os.ModePerm)
		files, _ := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(lvl))

		for _, file := range files {

			strArr := []rune((file).Name())
			gen, _ := strconv.Atoi(string(strArr[4:]))
			ss1 := GetSSTableParam(lvl, gen)
			ssts = append(ssts, ss1)

		}

	}

	for _, ss := range ssts {

		file, _ := os.Open(ss.sumFile.Filename)

		fr := bufio.NewReader(file)

		h := get_sum(fr)

		mins := string(h.minVal[:])

		var offset_ind *Index

		if min < mins {
			offset_ind = (Index).Decode(Index{}, fr)
		} else {

			offset_ind = findOffSum(min, ss.sumFile, 0)

			if offset_ind == nil {
				readers = append(readers, nil)
				continue
			}

		}

		file, _ = os.Open(ss.indexFile.Filename)
		file.Seek(int64(offset_ind.offset), 0)

		fr = bufio.NewReader(file)

		var start_index *Index

		start_index = nil

		for {

			i := (Index).Decode(Index{}, fr)

			if i == nil {
				break
			}

			if i.key >= min {
				start_index = i
				break

			}

		}

		if start_index == nil {
			readers = append(readers, nil)
			continue
		} else {
			file, _ := os.Open(ss.dataFile.Filename)
			file.Seek(int64(start_index.offset), 0)

			fr = bufio.NewReader(file)
			readers = append(readers, fr)
		}

	}

	for _, reader := range readers {
		if reader == nil {
			records_data = append(records_data, nil)
			continue
		}
		records_data = append(records_data, Decode(reader))
	}

	var r_upis *Record
	var min_ind int

	for {
		r_upis = nil

		for a, record := range records_data {

			if record != nil {
				r_upis = record
				min_ind = a
				break
			}

		}

		if r_upis == nil {
			break
		}

		for i := min_ind + 1; i < len(records_data); i++ {

			if records_data[i] == nil {
				continue
			} else if records_data[i].Key < r_upis.Key {
				r_upis = records_data[i]
				min_ind = i
			} else if records_data[i].Key == r_upis.Key {

				if records_data[i].Timestamp > r_upis.Timestamp {

					r_upis = records_data[i]
					records_data[min_ind] = Decode(readers[min_ind])
					min_ind = i

				}

			}

		}

		if !r_upis.Tombstone {
			if r_upis.Key <= max {
				if !In(r_upis.Key, &lista) {
					lista = append(lista, r_upis)
				}

			} else {

				break
			}
		}

		records_data[min_ind] = Decode(readers[min_ind])

	}

	fmt.Println(lista)

	return &lista

}

func In(key string, records *[]*Record) bool {

	for _, record := range *records {

		if key == record.Key {
			return true
		}
	}

	return false

}

func (SSTable) MergeInit() {

	first := -1

	for i := MAX_LVL; i > 0; i-- {

		os.MkdirAll(MAIN_DIR_FOLDERS+"/LVL"+strconv.Itoa(i), os.ModePerm)
		files, err := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(i))

		if err != nil {
			fmt.Println(err)
		}

		if len(files) != 0 {

			first = i
			break
		}

	}

	for i := 1; i <= MAX_LVL; i++ {

		os.MkdirAll(MAIN_DIR_FOLDERS+"/LVL"+strconv.Itoa(i), os.ModePerm)
		files, err := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(i))

		if err != nil {
			fmt.Println(err)
		}

		if len(files) >= lvlMap[i] {
			var index int
			slice := files[:len(files)-len(files)%lvlMap[i]]
			files_next, err2 := ioutil.ReadDir(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(i+1))
			if err2 != nil {
				(SSTable).Merge(SSTable{}, &slice, i, len(files)+1, i, false)
				continue
			}

			if (lvlMap[i+1] - (len(files_next) + (len(slice) / lvlMap[i]) + len(slice)%lvlMap[i])) < 0 {

				index = (lvlMap[i+1] - (len(files_next) + (len(slice) / lvlMap[i]) + len(slice)%lvlMap[i]))
			} else {
				index = len(files_next) + 1
			}

			var next_lvl int
			if i >= MAX_LVL {
				next_lvl = i
			} else {
				next_lvl = i + 1
			}

			var del bool

			if first == -1 {
				del = true
				first = i
			} else {
				if next_lvl > first {
					del = true
					first++
				} else {
					del = false
				}

			}

			if i+1 == MAX_LVL {

				(SSTable).Merge(SSTable{}, &slice, i+1, len(files_next)+1, i, del)
				continue

			}

			(SSTable).Merge(SSTable{}, &slice, next_lvl, index, i, del)

		}

	}

}

func (SSTable) Merge(files *[]fs.FileInfo, next_dir int, index int, this_dir int, del bool) {

	readers := make([]*bufio.Reader, 0)
	var bloom_sum uint
	files_to_close := make([]*os.File, 0)

	for buff := 0; buff < len(*files)-1; buff += lvlMap[this_dir] {

		for _, file := range (*files)[buff : buff+lvlMap[this_dir]] {

			strArr := []rune((file).Name())
			gen, _ := strconv.Atoi(string(strArr[4:]))
			ss1 := GetSSTableParam(this_dir, gen)
			bf := ss1.filterFile.read_bloom().GetElem(0.1)
			bloom_sum += bf

			file1, err := os.Open(ss1.dataFile.Filename)
			if err != nil {
				fmt.Println("NEMA")
			}
			files_to_close = append(files_to_close, file1)
			fr := bufio.NewReader(file1)
			readers = append(readers, fr)

		}

		merkle_r := CreateMerkleRoot()
		merkle_b := make([][]byte, 0)
		for i := range merkle_b {
			merkle_b[i] = make([]byte, 0)
		}

		offset := uint64(0)

		index_list := make([]*Index, 0)

		sst := GetSSTableParam(next_dir, index)
		index++

		file3, err := os.Create(sst.dataFile.Filename)
		if err != nil {
			fmt.Println("NEMA")
		}
		defer file3.Close()
		fw := bufio.NewWriter(file3)

		bloom := NewBloom(uint64(bloom_sum), 0.1)

		records := make([]*Record, 0)

		for _, reader := range readers {
			records = append(records, Decode(reader))
		}

		var r_upis *Record
		var min_ind int

		for {
			r_upis = nil

			for a, record := range records {

				if record != nil {
					r_upis = record
					min_ind = a
					break
				}

			}

			if r_upis == nil {
				break
			}

			for i := min_ind + 1; i < len(records); i++ {

				if records[i] == nil {
					continue
				} else if records[i].Key < r_upis.Key {
					r_upis = records[i]
					min_ind = i
				} else if records[i].Key == r_upis.Key {

					if records[i].Timestamp > r_upis.Timestamp {

						r_upis = records[i]
						records[min_ind] = Decode(readers[min_ind])
						min_ind = i

					}

				}
			}

			size := fw.Available()
			sst.dataFile.write_record(r_upis, fw)
			size_after := fw.Available()

			if r_upis.Tombstone && (del || next_dir == this_dir) {
				fw = bufio.NewWriter(file3)
			} else {

				bloom.Add(r_upis.Key)
				merkle_b = append(merkle_b, r_upis.Value)
				index := newIndex(r_upis.Keysize, r_upis.Key, offset)
				index_list = append(index_list, index)

				offset = uint64(size-size_after) + offset

			}

			records[min_ind] = Decode(readers[min_ind])

			fw.Flush()
		}

		for i, _ := range files_to_close {
			files_to_close[i].Close()
		}

		for _, file := range (*files)[buff : buff+lvlMap[this_dir]] {

			err := os.RemoveAll(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(this_dir) + "/" + file.Name() + "/")
			if err != nil {
				fmt.Println(err)
			}

		}

		if len(index_list) != 0 {
			sst.write_bloom(&bloom)
			sst.write_index(&index_list)
			merkle_r.FormMerkleTree(sst.metaPath, merkle_b, true)
			file3.Close()
		} else {
			file3.Close()
			err := os.RemoveAll(MAIN_DIR_FOLDERS + "/LVL" + strconv.Itoa(next_dir) + "/GEN-" + strconv.Itoa(index-1))
			if err != nil {
				fmt.Println(err)
			}

		}

		if this_dir == 1 || this_dir == 4 {
			Rename(this_dir)
		}
	}

}
