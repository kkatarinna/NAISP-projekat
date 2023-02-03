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
	"strconv"
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

// func getSSTableFile(index int) *SSTableFile {

// 	files, _ := ioutil.ReadDir(MAIN_DIR_FILES)
// 	// fmt.Println(len(files))
// 	i := len(files)

// 	if index < 0 || index > i {
// 		return nil
// 	}

// 	dir := MAIN_DIR_FILES + "/GEN-" + strconv.Itoa(index) + ".db"

// 	sst := &SSTableFile{}

// 	sst.sstFile = newBinaryFile(dir)

// 	return sst

// }

func RenameFile() {

	dir := MAIN_DIR_FILES + "/LVL1"
	files, _ := ioutil.ReadDir(dir)

	for i := 0; i < len(files); i++ {

		strArr := []rune((files)[i].Name())
		gen := string(strArr[4:])
		replace := dir + "/GEN-" + strconv.Itoa(i+1)
		os.Rename(dir+"/"+(files)[i].Name(), replace)
		os.Rename(replace+"/1usertable-"+gen+"-Data.db", replace+"/1usertable-"+strconv.Itoa(i+1)+"-Data.db")
		os.Rename(replace+"/1usertable-"+gen+"-Meta.txt", replace+"/1usertable-"+strconv.Itoa(i+1)+"-Meta.txt")

	}

}

func GetSSTableFileParam(lvl int, gen int) *SSTableFile {

	dir := MAIN_DIR_FILES + "/LVL" + strconv.Itoa(lvl) + "/GEN-" + strconv.Itoa(gen)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	sst := &SSTableFile{}

	str := dir + "/1usertable-" + strconv.Itoa(gen) + "-"

	sst.sstFile = newBinaryFile(str + "Data.db")

	sst.metaFile_path = str + "Meta.txt"

	sst.dataFile_offset = 0

	sst.indexFile_offset = 0

	sst.sumFile_offset = 0

	sst.filterFile_offset = 0

	return sst

}

func (sst *SSTableFile) Write_table(list *[]*Record) {

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

	for lvl := 1; lvl <= MAX_LVL; lvl++ {

		files, _ := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(lvl))
		// fmt.Println(len(files))
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
	}
	return nil
}

func (sst *SSTableFile) Encode() *bytes.Buffer {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, sst.dataFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.indexFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.sumFile_offset)
	binary.Write(&buffer, binary.LittleEndian, sst.filterFile_offset)

	return &buffer
}

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

func (SSTableFile) MergeInit() {

	for i := 1; i <= MAX_LVL; i++ {

		os.MkdirAll(MAIN_DIR_FILES+"/LVL"+strconv.Itoa(i), os.ModePerm)
		files, err := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(i))

		if err != nil {
			fmt.Println(err)
		}

		if len(files) >= lvlMap[i] {

			index := 1
			slice := files[:len(files)-len(files)%lvlMap[i]]
			files_next, err2 := ioutil.ReadDir(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(i+1))
			if err2 != nil {
				fmt.Print(err)
			}

			if (lvlMap[i+1] - (len(files_next) + len(slice)/2)) < 0 {

				index = (lvlMap[i+1] - (len(files_next) + len(slice)/2)) + 1
			} else {
				index = len(files_next) + 1
			}

			var next_lvl int
			if i >= MAX_LVL {
				next_lvl = i
			} else {
				next_lvl = i + 1
			}

			(SSTableFile).Merge(SSTableFile{}, &slice, next_lvl, index)

		}

	}

}

func (SSTableFile) Merge(files *[]fs.FileInfo, next_dir int, index int) {

	for i := 0; i < len(*files); i += 2 {

		bloom := NewBloom(100, 0.1)
		merkle_r := CreateMerkleRoot()
		merkle_b := make([][]byte, 0)

		offset := uint64(0)

		index_list := make([]*Index, 0)

		sst := GetSSTableFileParam(next_dir, index)

		file3, err := os.Create(sst.sstFile.Filename)
		if err != nil {
			fmt.Println("NEMA")
		}
		defer file3.Close()
		fw := bufio.NewWriter(file3)

		strArr := []rune((*files)[i].Name())
		gen, _ := strconv.Atoi(string(strArr[4:]))
		ss1 := GetSSTableFileParam(next_dir-1, gen)

		file1, err := os.Open(ss1.sstFile.Filename)
		if err != nil {
			fmt.Println("NEMA")
		}
		defer file1.Close()
		file1.Seek(-32, 2)
		fr := bufio.NewReader(file1)
		ss1 = ss1.Decode(fr, next_dir-1, gen)

		file1.Seek(0, 0)
		fr1 := bufio.NewReader(file1)

		strArr = []rune((*files)[i+1].Name())
		gen, _ = strconv.Atoi(string(strArr[4:]))
		ss2 := GetSSTableFileParam(next_dir-1, gen)
		file2, err := os.Open(ss2.sstFile.Filename)
		if err != nil {
			fmt.Println("NEMA")
		}
		defer file2.Close()
		file2.Seek(-32, 2)
		fr = bufio.NewReader(file2)
		ss2 = ss2.Decode(fr, next_dir-1, gen)

		file2.Seek(0, 0)
		fr2 := bufio.NewReader(file2)

		r1 := Decode(fr1)
		buff1 := 0

		r2 := Decode(fr2)
		buff2 := 0

		for {

			if uint64(buff1) >= ss1.indexFile_offset {

				for {

					if uint64(buff2) >= ss2.indexFile_offset {
						break
					}
					r_upis := r2
					bloom.Add(r_upis.Key)
					merkle_b = append(merkle_b, r_upis.Value)

					size := fw.Available()
					sst.sstFile.write_record(r_upis, fw)
					size_after := fw.Available()

					index := newIndex(r_upis.Keysize, r_upis.Key, offset)
					index_list = append(index_list, index)

					offset = uint64(size-size_after) + offset

					fw.Flush()
					buff2 += (size - size_after)
					if uint64(buff2) >= ss2.indexFile_offset {
						continue
					}

					r2 = Decode(fr2)

				}

				break

			}

			if uint64(buff2) >= ss2.indexFile_offset {

				for {

					if uint64(buff1) >= ss1.indexFile_offset {
						break
					}
					r_upis := r1
					bloom.Add(r_upis.Key)
					merkle_b = append(merkle_b, r_upis.Value)

					size := fw.Available()
					sst.sstFile.write_record(r_upis, fw)
					size_after := fw.Available()

					index := newIndex(r_upis.Keysize, r_upis.Key, offset)
					index_list = append(index_list, index)

					offset = uint64(size-size_after) + offset

					fw.Flush()
					buff1 += (size - size_after)
					if uint64(buff1) >= ss1.indexFile_offset {
						continue
					}

					r1 = Decode(fr1)

				}

				break

			}

			if r1.Key < r2.Key {

				r_upis := r1

				bloom.Add(r_upis.Key)
				merkle_b = append(merkle_b, r_upis.Value)

				size := fw.Available()
				sst.sstFile.write_record(r_upis, fw)
				size_after := fw.Available()

				index := newIndex(r_upis.Keysize, r_upis.Key, offset)
				index_list = append(index_list, index)

				offset = uint64(size-size_after) + offset

				fw.Flush()

				buff1 += (size - size_after)
				if uint64(buff1) >= ss1.indexFile_offset {
					continue
				}

				r1 = Decode(fr1)

			} else if r1.Key == r2.Key {

				r_upis := r1

				if r1.Timestamp < r2.Timestamp {
					r_upis = r2

				}

				bloom.Add(r_upis.Key)
				merkle_b = append(merkle_b, r_upis.Value)

				size := fw.Available()
				sst.sstFile.write_record(r_upis, fw)
				size_after := fw.Available()

				index := newIndex(r_upis.Keysize, r_upis.Key, offset)
				index_list = append(index_list, index)

				offset = uint64(size-size_after) + offset

				fw.Flush()
				buff2 += (size - size_after)
				buff1 += (size - size_after)
				if uint64(buff2) >= ss2.indexFile_offset || uint64(buff1) >= ss1.indexFile_offset {
					continue
				}

				r1 = Decode(fr1)
				r2 = Decode(fr2)

			} else if r1.Key > r2.Key {

				r_upis := r2

				bloom.Add(r_upis.Key)
				merkle_b = append(merkle_b, r_upis.Value)

				size := fw.Available()
				sst.sstFile.write_record(r_upis, fw)
				size_after := fw.Available()

				index := newIndex(r_upis.Keysize, r_upis.Key, offset)
				index_list = append(index_list, index)

				offset = uint64(size-size_after) + offset

				fw.Flush()
				buff2 += (size - size_after)
				if uint64(buff2) >= ss2.indexFile_offset {
					continue
				}

				r2 = Decode(fr2)

			}

		}
		sst.indexFile_offset = offset
		sst.write_index(&index_list, &offset, fw)
		sst.write_bloom(&bloom, &offset, fw)
		sst.dataFile_offset = offset

		sst.write_offsets(fw)
		fmt.Println(4096 - fw.Available())
		fw.Flush()
		file3.Close()
		merkle_r.FormMerkleTree(sst.metaFile_path, merkle_b, true)

		err = os.RemoveAll(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(next_dir-1) + "/" + (*files)[i].Name() + "/")
		fmt.Println(err)
		os.RemoveAll(MAIN_DIR_FILES + "/LVL" + strconv.Itoa(next_dir-1) + "/" + (*files)[i+1].Name() + "/")

	}

	if next_dir == 2 {
		RenameFile()
	}

}
