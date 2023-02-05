package sstable

//klasa za sst fajlove sa fajl organizacijom
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

const MAIN_DIR_FILES = "./Data/SSTable_Data/SST_Files"

type SSTableFile struct {
	sstFile           *BinaryFile
	metaFile_path     string
	dataFile_offset   uint64
	indexFile_offset  uint64
	sumFile_offset    uint64
	filterFile_offset uint64
}

// konstruktor
func NewSSTableFile() *SSTableFile {

	files, err := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL1")

	if err != nil {
		fmt.Print(err)
	}

	i := len(files) + 1

	dir := MAIN_DIR_FILES + "/LVL1" + "/GEN-" + strconv.Itoa(i)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTableFile{}

	str := dir + "/1usertable-" + strconv.Itoa(i) + "-"

	sst.sstFile = newBinaryFile(str + "Data.db")

	sst.metaFile_path = str + "Meta.txt"

	sst.dataFile_offset = 0

	sst.indexFile_offset = 0

	sst.sumFile_offset = 0

	sst.filterFile_offset = 0

	return sst

}

// pravilno indeksira preostale fajlove u folderu nakon merga
func RenameFile(this_dir int) {

	dir := MAIN_DIR_FILES + "/LVL" + strconv.Itoa(this_dir)
	files, _ := ioutil.ReadDir(dir)

	for i := 0; i < len(files); i++ {

		strArr := []rune((files)[i].Name())
		gen := string(strArr[4:])
		replace := dir + "/GEN-" + strconv.Itoa(i+1)
		os.Rename(dir+"/"+(files)[i].Name(), replace)
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Data.db", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Data.db")
		os.Rename(replace+"/"+strconv.Itoa(this_dir)+"usertable-"+gen+"-Meta.txt", replace+"/"+strconv.Itoa(this_dir)+"usertable-"+strconv.Itoa(i+1)+"-Meta.txt")

	}

}

// vraca odredjen sst fajl u odnosu na date parametre
func GetSSTableFileParam(lvl int, gen int) *SSTableFile {

	dir := MAIN_DIR_FILES + "/LVL" + strconv.Itoa(lvl) + "/GEN-" + strconv.Itoa(gen)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTableFile{}

	str := dir + "/" + strconv.Itoa(lvl) + "usertable-" + strconv.Itoa(gen) + "-"

	sst.sstFile = newBinaryFile(str + "Data.db")

	sst.metaFile_path = str + "Meta.txt"

	sst.dataFile_offset = 0

	sst.indexFile_offset = 0

	sst.sumFile_offset = 0

	sst.filterFile_offset = 0

	return sst

}

// pocetna funkcije pisanja sstabele
func (sst *SSTableFile) Write_table(list *[]*Record) {

	sort.Slice(*list, func(i, j int) bool {
		return (*list)[i].Key < (*list)[j].Key
	})

	bloom := NewBloom(uint64(len(*list)), 0.1)
	merkle_r := CreateMerkleRoot()
	merkle_b := make([][]byte, 0)
	for i := range merkle_b {
		merkle_b[i] = make([]byte, 0)
	}

	file, err := os.Create(sst.sstFile.Filename)
	if err != nil {
		fmt.Println("err data file")
	}

	fw := bufio.NewWriter(file)

	offset := uint64(0)

	index_list := make([]*Index, 0)

	for _, element := range *list {

		bloom.Add(element.Key)
		merkle_b = append(merkle_b, element.Value)

		size := fw.Available()
		sst.sstFile.write_record(element, fw)
		size_after := fw.Available()

		index := newIndex(element.Keysize, element.Key, offset)
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
	merkle_r.FormMerkleTree(sst.metaFile_path, merkle_b, true)

}

// pisanje indexa
func (sst *SSTableFile) write_index(list *[]*Index, prev_offset *uint64, fw *bufio.Writer) {

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
		sst.sstFile.write_index(element, fw)
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

	*prev_offset += offset
	sst.sumFile_offset = *prev_offset

	sst.write_summary(sum, &index_list, prev_offset, fw)

}

// pisanje summarya
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

// pisanje blooma
func (sst *SSTableFile) write_bloom(bloom *Bloom, prev_offset *uint64, fw *bufio.Writer) {

	size := fw.Available()
	sst.sstFile.write_bloom(bloom, fw)
	size_after := fw.Available()

	fw.Flush()

	*prev_offset += uint64(size - size_after)

}

// pisanje offseta svih "fajlova"  u glavnom gajlu
func (sst *SSTableFile) write_offsets(fw *bufio.Writer) {

	binary_data := sst.Encode()
	binary.Write(fw, binary.LittleEndian, binary_data.Bytes())

}

// Obicna pretraga
func (SSTableFile) Find_record(key string) *Record {

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		files, _ := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(lvl))

		i := len(files)

		for ; i > 0; i-- {

			ss := GetSSTableFileParam(lvl, i)

			file, _ := os.Open(ss.sstFile.Filename)

			file.Seek(-32, 2)
			fr := bufio.NewReader(file)

			ss = ss.Decode(fr, lvl, i)

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

			rec_ind := findOffInd(key, ss.sstFile, ss.indexFile_offset+offset_ind.offset, ss.sumFile_offset-ss.indexFile_offset)

			if rec_ind == nil {
				continue
			}

			file.Seek(int64(rec_ind.offset), 0)
			fr = bufio.NewReader(file)

			record := Decode(fr)

			fmt.Println(record)

			return record

		}
	}
	return nil
}

// prefix pretraga
func (SSTableFile) List(key string, records_mem *[]*Record) *[]*Record {

	var buffer uint64
	var bytess bytes.Buffer
	var bytes_mem bytes.Buffer

	lista := make([]*Record, 0)
	ssts := make([]*SSTableFile, 0)
	readers := make([]*bufio.Reader, 0)
	buffers := make([]uint64, 0)
	records_data := make([]*Record, 0)

	for _, rec := range *records_mem {

		binary.Write(&bytes_mem, binary.LittleEndian, rec.Encode().Bytes())

	}
	fr := bufio.NewReader(&bytes_mem)
	readers = append(readers, (*bufio.Reader)(fr))
	buffers = append(buffers, 0)

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		os.MkdirAll(MAIN_DIR_FILES+"/LVL"+strconv.Itoa(lvl), os.ModePerm)
		files, _ := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(lvl))

		for _, file := range files {

			strArr := []rune((file).Name())
			gen, _ := strconv.Atoi(string(strArr[4:]))
			ss1 := GetSSTableFileParam(lvl, gen)

			file1, err := os.Open(ss1.sstFile.Filename)
			if err != nil {
				fmt.Println("NEMA")
			}
			defer file1.Close()

			file1.Seek(-32, 2)
			fr := bufio.NewReader(file1)
			ss1 = ss1.Decode(fr, lvl, gen)
			ssts = append(ssts, ss1)

		}

	}

	for _, ss := range ssts {

		file, _ := os.Open(ss.sstFile.Filename)

		file.Seek(int64(ss.sumFile_offset), 0)

		fr := bufio.NewReader(file)

		h := get_sum(fr)

		min := string(h.minVal[:])

		var offset_ind *Index

		if key < min {
			offset_ind = (Index).Decode(Index{}, fr)
		} else {
			file.Seek(int64(ss.sumFile_offset), 0)

			offset_ind = findOffSum(key, ss.sstFile, ss.sumFile_offset)

			if offset_ind == nil {
				readers = append(readers, nil)
				buffers = append(buffers, 0)
				continue
			}

		}

		file.Seek(int64(ss.indexFile_offset+offset_ind.offset), 0)

		fr = bufio.NewReader(file)

		var start_index *Index

		start_index = nil

		buffer = ss.indexFile_offset + offset_ind.offset

		for {

			if buffer >= ss.sumFile_offset {
				break
			}

			i := (Index).Decode(Index{}, fr)
			bytess = *i.Encode()
			buffer += uint64(bytess.Len())

			if strings.HasPrefix(i.key, key) {
				start_index = i
				break

			}

		}

		if start_index == nil {
			readers = append(readers, nil)
			buffers = append(buffers, 0)
			continue
		} else {
			file, _ := os.Open(ss.sstFile.Filename)
			file.Seek(int64(start_index.offset), 0)

			fr = bufio.NewReader(file)
			buffer = start_index.offset
			buffers = append(buffers, buffer)
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

			if record == nil {
				continue
			}

			if a == 0 {

				r_upis = record
				min_ind = a
				break

			}

			if buffers[a] < ssts[a-1].indexFile_offset {
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
			}

			if buffers[i] >= ssts[i-1].indexFile_offset {
				continue
			} else if records_data[i].Key < r_upis.Key {
				r_upis = records_data[i]
				min_ind = i
			} else if records_data[i].Key == r_upis.Key {

				if records_data[i].Timestamp > r_upis.Timestamp {

					bytess = *r_upis.Encode()

					r_upis = records_data[i]
					if min_ind == 0 {
						records_data[min_ind] = Decode(readers[min_ind])
					} else {
						buffers[min_ind] += uint64(bytess.Len())
						if uint64(buffers[min_ind]) >= ssts[min_ind].indexFile_offset {
							min_ind = i
							continue
						}
					}
					records_data[min_ind] = Decode(readers[min_ind])
					min_ind = i

				} else {
					bytess = *records_data[i].Encode()

					buffers[i] += uint64(bytess.Len())
					if uint64(buffers[i]) >= ssts[i-1].indexFile_offset {
						continue
					}
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

		bytess = *records_data[min_ind].Encode()
		buffers[min_ind] += uint64(bytess.Len())

		if min_ind != 0 {
			if buffers[min_ind] >= ssts[min_ind-1].indexFile_offset {
				continue
			}

		}

		records_data[min_ind] = Decode(readers[min_ind])

	}

	fmt.Println(lista)

	return &lista

}

// range pretraga
func (SSTableFile) Range(min string, max string, records_mem *[]*Record) *[]*Record {

	var buffer uint64
	var bytess bytes.Buffer
	var bytes_mem bytes.Buffer

	lista := make([]*Record, 0)
	ssts := make([]*SSTableFile, 0)
	readers := make([]*bufio.Reader, 0)
	buffers := make([]uint64, 0)
	records_data := make([]*Record, 0)

	for _, rec := range *records_mem {

		binary.Write(&bytes_mem, binary.LittleEndian, rec.Encode().Bytes())

	}
	fr := bufio.NewReader(&bytes_mem)
	readers = append(readers, (*bufio.Reader)(fr))
	buffers = append(buffers, 0)

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		os.MkdirAll(MAIN_DIR_FILES+"/LVL"+strconv.Itoa(lvl), os.ModePerm)
		files, _ := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(lvl))

		for _, file := range files {

			strArr := []rune((file).Name())
			gen, _ := strconv.Atoi(string(strArr[4:]))
			ss1 := GetSSTableFileParam(lvl, gen)

			file1, err := os.Open(ss1.sstFile.Filename)
			if err != nil {
				fmt.Println("NEMA")
			}
			defer file1.Close()

			file1.Seek(-32, 2)
			fr := bufio.NewReader(file1)
			ss1 = ss1.Decode(fr, lvl, gen)
			ssts = append(ssts, ss1)

		}

	}

	for _, ss := range ssts {

		file, _ := os.Open(ss.sstFile.Filename)

		file.Seek(int64(ss.sumFile_offset), 0)

		fr := bufio.NewReader(file)

		h := get_sum(fr)

		mins := string(h.minVal[:])

		var offset_ind *Index

		if min < mins {
			offset_ind = (Index).Decode(Index{}, fr)
		} else {
			file.Seek(int64(ss.sumFile_offset), 0)

			offset_ind = findOffSum(min, ss.sstFile, ss.sumFile_offset)

			if offset_ind == nil {
				readers = append(readers, nil)
				buffers = append(buffers, 0)
				continue
			}

		}

		file.Seek(int64(ss.indexFile_offset+offset_ind.offset), 0)

		fr = bufio.NewReader(file)

		var start_index *Index

		start_index = nil

		buffer = ss.indexFile_offset + offset_ind.offset

		for {

			if buffer >= ss.sumFile_offset {
				break
			}

			i := (Index).Decode(Index{}, fr)
			bytess = *i.Encode()
			buffer += uint64(bytess.Len())

			if i.key >= min {
				start_index = i
				break
			}

		}

		if start_index == nil {
			readers = append(readers, nil)
			buffers = append(buffers, 0)
			continue
		} else {
			file, _ := os.Open(ss.sstFile.Filename)
			file.Seek(int64(start_index.offset), 0)

			fr = bufio.NewReader(file)
			buffer = start_index.offset
			buffers = append(buffers, buffer)
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

			if record == nil {
				continue
			}

			if a == 0 {

				r_upis = record
				min_ind = a
				break

			}

			if buffers[a] < ssts[a-1].indexFile_offset {
				r_upis = record
				min_ind = a
				break
			}

		}

		if r_upis == nil {
			break
		}

		for i := min_ind + 1; i < len(records_data); i++ {

			if records_data == nil {
				continue
			}

			if buffers[i] >= ssts[i-1].indexFile_offset {
				continue
			} else if records_data[i].Key < r_upis.Key {
				r_upis = records_data[i]
				min_ind = i
			} else if records_data[i].Key == r_upis.Key {

				if records_data[i].Timestamp > r_upis.Timestamp {

					bytess = *r_upis.Encode()

					r_upis = records_data[i]
					if min_ind == 0 {
						records_data[min_ind] = Decode(readers[min_ind])
					} else {
						buffers[min_ind] += uint64(bytess.Len())
						if uint64(buffers[min_ind]) >= ssts[min_ind-1].indexFile_offset {
							min_ind = i
							continue
						}
					}
					records_data[min_ind] = Decode(readers[min_ind])
					min_ind = i

				} else {
					bytess = *records_data[i].Encode()

					buffers[i] += uint64(bytess.Len())
					if uint64(buffers[i]) >= ssts[i-1].indexFile_offset {
						continue
					}
					records_data[i] = Decode(readers[i])

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

		bytess = *records_data[min_ind].Encode()
		buffers[min_ind] += uint64(bytess.Len())

		if min_ind != 0 {
			if buffers[min_ind] >= ssts[min_ind-1].indexFile_offset {
				continue
			}

		}

		records_data[min_ind] = Decode(readers[min_ind])

	}

	fmt.Println(lista)

	return &lista

}

// serijalizacija sstabele
func (sst *SSTableFile) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, sst.dataFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.indexFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.sumFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.filterFile_offset)

	return &buffer
}

// deserijalizacija sstabele
func (SSTableFile) Decode(fr *bufio.Reader, lvl int, gen int) *SSTableFile {

	sst := GetSSTableFileParam(lvl, gen)

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

	return sst
}

// pocetak mergea
func (SSTableFile) MergeInit() {

	first := -1

	for i := MAX_LVL; i > 0; i-- {

		os.MkdirAll(MAIN_DIR_FILES+"/LVL"+strconv.Itoa(i), os.ModePerm)
		files, err := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(i))

		if err != nil {
			fmt.Println(err)
		}

		if len(files) != 0 {

			first = i
			break
		}

	}

	for i := 1; i <= MAX_LVL; i++ {

		os.MkdirAll(MAIN_DIR_FILES+"/LVL"+strconv.Itoa(i), os.ModePerm)
		files, err := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(i))

		if err != nil {
			fmt.Println(err)
		}

		if len(files) >= lvlMap[i] {
			var index int
			slice := files[:len(files)-len(files)%lvlMap[i]]
			files_next, err2 := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(i+1))
			if err2 != nil {
				(SSTableFile).Merge(SSTableFile{}, &slice, i, len(files)+1, i, false)
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

				(SSTableFile).Merge(SSTableFile{}, &slice, i+1, len(files_next)+1, i, del)
				continue

			}

			(SSTableFile).Merge(SSTableFile{}, &slice, next_lvl, index, i, del)

		}

	}

}

// merge
func (SSTableFile) Merge(files *[]fs.FileInfo, next_dir int, index int, this_dir int, del bool) {

	var bloom_sum uint

	for buff := 0; buff < len(*files)-1; buff += lvlMap[this_dir] {

		ssts := make([]*SSTableFile, 0)
		readers := make([]*bufio.Reader, 0)
		buffers := make([]uint64, 0)
		files_to_close := make([]*os.File, 0)

		for _, file := range (*files)[buff : buff+lvlMap[this_dir]] {

			strArr := []rune((file).Name())
			gen, _ := strconv.Atoi(string(strArr[4:]))
			ss1 := GetSSTableFileParam(this_dir, gen)

			file1, err := os.Open(ss1.sstFile.Filename)
			if err != nil {
				fmt.Println("NEMA")
			}
			files_to_close = append(files_to_close, file1)

			file1.Seek(-32, 2)
			fr := bufio.NewReader(file1)
			ss1 = ss1.Decode(fr, this_dir, gen)
			ssts = append(ssts, ss1)

			file1.Seek(int64(ss1.filterFile_offset), 0)
			fr = bufio.NewReader(file1)
			bf := Get_bloom(fr).GetElem(0.1)

			bloom_sum += bf

			file1.Seek(0, 0)
			fr = bufio.NewReader(file1)
			readers = append(readers, fr)

		}

		merkle_r := CreateMerkleRoot()
		merkle_b := make([][]byte, 0)
		for i := range merkle_b {
			merkle_b[i] = make([]byte, 0)
		}

		offset := uint64(0)

		index_list := make([]*Index, 0)

		sst := GetSSTableFileParam(next_dir, index)
		index++

		file3, err := os.Create(sst.sstFile.Filename)
		if err != nil {
			fmt.Println("NEMA")
		}
		defer file3.Close()
		fw := bufio.NewWriter(file3)

		bloom := NewBloom(uint64(bloom_sum), 0.1)

		records := make([]*Record, 0)

		for _, reader := range readers {
			records = append(records, Decode(reader))
			buffers = append(buffers, 0)
		}

		var r_upis *Record
		var min_ind int

		for {
			r_upis = nil

			for a, record := range records {

				if buffers[a] < ssts[a].indexFile_offset {
					r_upis = record
					min_ind = a
					break
				}

			}

			if r_upis == nil {
				break
			}

			for i := min_ind + 1; i < len(records); i++ {

				if buffers[i] >= ssts[i].indexFile_offset {
					continue
				} else if records[i].Key < r_upis.Key {
					r_upis = records[i]
					min_ind = i
				} else if records[i].Key == r_upis.Key {

					if records[i].Timestamp > r_upis.Timestamp {

						size := fw.Available()
						sst.sstFile.write_record(r_upis, fw)
						size_after := fw.Available()

						r_upis = records[i]
						buffers[min_ind] += uint64(size - size_after)
						if uint64(buffers[min_ind]) >= ssts[min_ind].indexFile_offset {
							min_ind = i
							fw = bufio.NewWriter(file3)
							continue
						}
						records[min_ind] = Decode(readers[min_ind])
						min_ind = i
						fw = bufio.NewWriter(file3)

					}

				}
			}

			size := fw.Available()
			sst.sstFile.write_record(r_upis, fw)
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

			fw.Flush()
			buffers[min_ind] += uint64(size - size_after)
			if uint64(buffers[min_ind]) >= ssts[min_ind].indexFile_offset {
				continue
			}
			records[min_ind] = Decode(readers[min_ind])
		}
		for i, _ := range files_to_close {
			files_to_close[i].Close()
		}

		for _, file := range (*files)[buff : buff+lvlMap[this_dir]] {

			err := os.RemoveAll(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(this_dir) + "/" + file.Name() + "/")
			if err != nil {
				fmt.Println(err)
			}

		}

		sst.indexFile_offset = offset
		if len(index_list) != 0 {

			sst.write_index(&index_list, &offset, fw)
			sst.write_bloom(&bloom, &offset, fw)
			sst.dataFile_offset = offset

			sst.write_offsets(fw)
			fmt.Println(4096 - fw.Available())
			fw.Flush()
			merkle_r.FormMerkleTree(sst.metaFile_path, merkle_b, true)
			file3.Close()

		} else {
			file3.Close()
			err := os.RemoveAll(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(next_dir) + "/GEN-" + strconv.Itoa(index-1))
			if err != nil {
				fmt.Println(err)
			}

		}

	}

	if this_dir == 1 || this_dir == 4 {
		RenameFile(this_dir)
	}

}
