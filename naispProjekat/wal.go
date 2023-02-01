
package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"encoding/binary"
	"bytes"
	"io/ioutil"
	"io"
	"strconv"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"hash/crc32"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE = 4
	TIMESTAMP_SIZE = 8
	TOMBSTONE_SIZE = 1
	KEY_SIZE_SIZE = 8
	VALUE_SIZE_SIZE = 8
	
	CRC_START = 0
	TIMESTAMP_START = CRC_START + CRC_SIZE
	TOMBSTONE_START = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START = VALUE_SIZE_START + VALUE_SIZE_SIZE

)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type segment struct {
	crc uint32
	timestamp uint64
	tombstone uint64
	keysize uint64
	valuesize uint64
	key string
	value string
}

func main() {

	// // deletes wal
	// err := deleteWal()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("")


	// //is wal empty
	// res, _ := isWalEmpty()
	// fmt.Println(res)



	//find active file
	activeFile, err := getActiveFile()
	if err != nil {
		log.Fatal(err)
	}
	//convert data to binary
	dataBinary := dataToBinary(0, "key2", "value2")
	completeActiveFile := "wal/" + activeFile

	//append
	f, err := os.OpenFile(completeActiveFile, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	err = appendData(f, dataBinary)
	if err != nil {
		log.Fatal(err)
	}

	//read
	data, err := readAll()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(data)

}

func readAll() ([]segment, error) {
	allDataElem := []segment{}

	ff, err := os.Open("wal")
    if err != nil {
        return nil, err
    }
    fileInfo, err := ff.Readdir(-1)
    ff.Close()
    if err != nil {
        return nil, err
    }

	for _, file := range fileInfo {
		filepath := "wal/" + file.Name()
		f, err := os.OpenFile(filepath, os.O_RDWR | os.O_CREATE, 0644)
		if err != nil {log.Fatal(err)}
		dataElem, err := read(f)
		if err != nil {
			return nil, err
		}
		f.Close()
		allDataElem = append(allDataElem, dataElem...)
	}
	return allDataElem, nil
}

func read(file *os.File) ([]segment, error) {
	dataElem := []segment{}
    
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {return nil, err}
	defer mmapf.Unmap()
	result := make([]byte, len(mmapf))
	copy(result, mmapf)

	fileLength, err := fileLen(file)
	if err != nil {return nil, err}

	start := 0
	for {
		crc := binary.BigEndian.Uint32(result[start:start+4])

		timestamp := binary.BigEndian.Uint64(result[start+4:start+12])

		buf := bytes.NewBuffer(result[start+12:start+13])
		tombstone, _ := binary.ReadUvarint(buf)
		
		keySize := binary.BigEndian.Uint64(result[start+13:start+21])

		valueSize := binary.BigEndian.Uint64(result[start+21:start+29])
		
		key := string(result[start+29:start+29+int(keySize)])
		value := string(result[start+29+int(keySize):start+29+int(keySize)+int(valueSize)])

		//test if data is damaged
		crcTest := CRC32([]byte(value))
		if crcTest != crc {
			panic("error occured")
		}
		
		currentElem := segment{crc: crc, timestamp: timestamp, tombstone: tombstone, keysize: keySize, valuesize: valueSize, key: key, value: value }
		dataElem = append(dataElem, currentElem)

		start = start+29+int(keySize)+int(valueSize)
		if start >= int(fileLength) {
			break
		}
	}


	return dataElem, nil
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil { return 0, err}
	return info.Size(), nil
}

func dataToBinary(tombStone uint64, key string, value string) ([]byte) {
	dataBinary := make([]byte, 29)

	crc := CRC32([]byte(value))
	timestamp := time.Now().Unix()
	binary.BigEndian.PutUint32(dataBinary[CRC_START:], crc)
	binary.BigEndian.PutUint64(dataBinary[TIMESTAMP_START:], uint64(timestamp))
	n := binary.PutUvarint(dataBinary[TOMBSTONE_START:], tombStone)
	if n < 0 {
		panic("error occured")
	}
	binary.BigEndian.PutUint64(dataBinary[KEY_SIZE_START:], uint64(len([]byte(key))))
	binary.BigEndian.PutUint64(dataBinary[VALUE_SIZE_START:], uint64(len([]byte(value))))

	dataToBeJoined := [][]byte{dataBinary, []byte(key), []byte(value)}
	dataJoined := bytes.Join(dataToBeJoined, []byte(""))
	
	return dataJoined
}

func listAllFiles() ([]string, error) {
	var allFiles []string
	files, err := ioutil.ReadDir("wal/")
    if err != nil {
        return nil, err
    }

    for _, file := range files {
		allFiles = append(allFiles, file.Name())
    }
	return allFiles, nil
}

func getNumberOfSegments(activeFile string) (int) {
	f, err := os.OpenFile(activeFile, os.O_RDWR, 0644)
	if err != nil {log.Fatal(err)}

	data, err := read(f)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	return len(data)
}

func getActiveFile() (string, error) {
	allFiles, err := listAllFiles()
    if err != nil {
        return "", err
    }
	
	if len(allFiles) == 0 {
		return "wal_1.log",  nil
	} else {
		activeFile := "wal/" + allFiles[len(allFiles)-1]

		if getNumberOfSegments(activeFile) < 3 {
			return allFiles[len(allFiles)-1], nil
		}

		fileNumber := strconv.FormatInt(int64(len(allFiles)+1), 10)
		newFileName := "wal_" + fileNumber + ".log"
		return newFileName, nil
		
	}
}

func appendData(file *os.File, data []byte) error {
	fileLength, err := fileLen(file)
	if err != nil {return err}
	err = file.Truncate(fileLength + int64(len(data)))
	if err != nil {return err}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {return err}
	defer mmapf.Unmap()
	copy(mmapf[fileLength:], data)
	mmapf.Flush()
	return nil
}

func deleteWal() error {
	d, err := os.Open("wal/")
    if err != nil {
        return err
    }
    defer d.Close()
    names, err := d.Readdirnames(-1)
    if err != nil {
        return err
    }
    for _, name := range names {
        err = os.RemoveAll(filepath.Join("wal/", name))
        if err != nil {
            return err
        }
    }
    return nil
}

func isWalEmpty() (bool, error) {
	f, err := os.Open("wal/")
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