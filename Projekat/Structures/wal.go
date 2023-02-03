package structures

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
	. "projekat/Structures/SSTable"

	"hash/crc32"

	"github.com/edsrzf/mmap-go"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this Record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type ConfigWal struct {
	
}

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32Wal(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// type Record struct {
// 	CrcW       uint32
// 	timestampW uint64
// 	tombstoneW bool
// 	keysizeW   uint64
// 	valuesizeW uint64
// 	keyW       string
// 	valueW     []byte
// }

// func main() {

// 	// // deletes wal
// 	// err := deleteWal()
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// fmt.Println("")

// 	// //is wal empty
// 	// res, _ := isWalEmpty()
// 	// fmt.Println(res)

// //append Record
// success := AppendRecordWal(config, false, "key3", []byte("value3"))
// fmt.Println(success)
// success2 := AppendRecordWal(config, false, "key2", []byte("value3"))
// fmt.Println(success2)
// success3 := AppendRecordWal(config, true, "key3", []byte("value3"))
// fmt.Println(success3)

// //read
// data, err := ReadAllWal()
// if err != nil {
// 	log.Fatal(err)
// }
// fmt.Println(data)

// }

func ReadAllWal() ([]Record, error) {
	allDataElem := []Record{}

	ff, err := os.Open("./Data/wal/")
	if err != nil {
		return nil, err
	}
	fileInfo, err := ff.Readdir(-1)
	ff.Close()
	if err != nil {
		return nil, err
	}

	for _, file := range fileInfo {
		filepath := "./Data/wal/" + file.Name()
		f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		dataElem, err := readWal(f)
		if err != nil {
			return nil, err
		}
		f.Close()
		allDataElem = append(allDataElem, dataElem...)
	}
	return allDataElem, nil
}

func readWal(file *os.File) ([]Record, error) {
	dataElem := []Record{}

	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	result := make([]byte, len(mmapf))
	copy(result, mmapf)

	fileLength, err := fileLenWal(file)
	if err != nil {
		return nil, err
	}

	start := 0
	for {
		crc := binary.BigEndian.Uint32(result[start : start+4])

		timestamp := binary.BigEndian.Uint64(result[start+4 : start+12])

		var tombstone bool
		buf := bytes.NewBuffer(result[start+12 : start+13])
		tombstoneByte, _ := binary.ReadUvarint(buf)
		if tombstoneByte == 1 {
			tombstone = true
		}
		// tombstone := result[12]

		keySize := binary.BigEndian.Uint64(result[start+13 : start+21])

		valueSize := binary.BigEndian.Uint64(result[start+21 : start+29])

		key := string(result[start+29 : start+29+int(keySize)])
		value := []byte(string(result[start+29+int(keySize) : start+29+int(keySize)+int(valueSize)]))

		//test if data is damaged
		crcTest := CRC32Wal(value)
		if crcTest != crc {
			panic("error occured")
		}

		currentElem := Record{Crc: crc, Timestamp: timestamp, Tombstone: tombstone, Keysize: keySize, Valuesize: valueSize, Key: key, Value: value}
		dataElem = append(dataElem, currentElem)

		start = start + 29 + int(keySize) + int(valueSize)
		if start >= int(fileLength) {
			break
		}
	}

	return dataElem, nil
}

func fileLenWal(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func AppendRecordWal(config *Config, tombStone bool, key string, value []byte) bool {
	//find active file
	activeFile, err := getActiveFile(config)
	if err != nil {
		return false
	}
	//convert data to binary
	dataBinary := dataToBinaryWal(tombStone, key, value)
	completeActiveFile := "./Data/wal/" + activeFile

	//append
	f, err := os.OpenFile(completeActiveFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return false
	}
	defer f.Close()

	err = appendDataWal(f, dataBinary)
	if err != nil {
		return false
	}
	return true
}

func dataToBinaryWal(tombStone bool, key string, value []byte) []byte {
	dataBinary := make([]byte, 29)

	crc := CRC32Wal(value)
	timestamp := time.Now().Unix()
	binary.BigEndian.PutUint32(dataBinary[CRC_START:], crc)
	binary.BigEndian.PutUint64(dataBinary[TIMESTAMP_START:], uint64(timestamp))

	if tombStone {
		n := binary.PutUvarint(dataBinary[TOMBSTONE_START:], 1)
		if n < 0 {
			panic("error occured")
		}
	} else {
		n := binary.PutUvarint(dataBinary[TOMBSTONE_START:], 0)
		if n < 0 {
			panic("error occured")
		}
	}

	binary.BigEndian.PutUint64(dataBinary[KEY_SIZE_START:], uint64(len([]byte(key))))
	binary.BigEndian.PutUint64(dataBinary[VALUE_SIZE_START:], uint64(len(value)))

	dataToBeJoined := [][]byte{dataBinary, []byte(key), value}
	dataJoined := bytes.Join(dataToBeJoined, []byte(""))

	return dataJoined
}

func listAllFilesWal() ([]string, error) {
	var allFiles []string
	files, err := ioutil.ReadDir("./Data/wal/")
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		allFiles = append(allFiles, file.Name())
	}
	return allFiles, nil
}

func getNumberOfRecordsWal(activeFile string) int {
	f, err := os.OpenFile(activeFile, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}

	data, err := readWal(f)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	return len(data)
}

func getActiveFile(config *Config) (string, error) {
	allFiles, err := listAllFilesWal()
	if err != nil {
		return "", err
	}

	if len(allFiles) == 0 {
		return "wal_1.log", nil
	} else {
		activeFile := "./Data/wal/" + allFiles[len(allFiles)-1]

		if getNumberOfRecordsWal(activeFile) < config.SegmentSize {
			return allFiles[len(allFiles)-1], nil
		}

		fileNumber := strconv.FormatInt(int64(len(allFiles)+1), 10)
		newFileName := "wal_" + fileNumber + ".log"
		return newFileName, nil

	}
}

func appendDataWal(file *os.File, data []byte) error {
	fileLength, err := fileLenWal(file)
	if err != nil {
		return err
	}
	err = file.Truncate(fileLength + int64(len(data)))
	if err != nil {
		return err
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[fileLength:], data)
	mmapf.Flush()
	return nil
}

func DeleteWal() error {
	d, err := os.Open("./Data/wal/")
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join("./Data/wal/", name))
		if err != nil {
			return err
		}
	}
	return nil
}

func IsWalEmpty() (bool, error) {
	f, err := os.Open("./Data/wal/")
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
