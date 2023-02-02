package structures
// 1 kom
import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"os"
	"log"
	"bufio"
	"strings"
)

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

// func main() {
// 	// fmt.Println(GetMD5Hash("hello"))
// 	// fmt.Println(ToBinary(GetMD5Hash("hello")))

// 	file1 := LoadFile("simhash/tekst1.txt")
// 	file2 := LoadFile("simhash/tekst2.txt")
// 	counter := HammingDifference(file1, file2)
// 	fmt.Println(counter)
// }

func LoadFile(file string) ([256]int) {
	words := []string{}
	weight := make(map[string]int)

	f, err := os.Open(file)
	if err != nil {log.Fatal(err)}
	defer f.Close()

	//split words

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		s := scanner.Text()
		s = strings.ToLower(s)
		words = append(words, s)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	//calculate weight
	for _, word := range words {
        if word[len(word)-1] == '.' || word[len(word)-1] == ':' || word[len(word)-1] == ',' {
			word = word[:len(word)-1]
		}

		val, ok := weight[word]
		if !ok {
			weight[word] = 1
		} else {
			weight[word] = val + 1
		}
    }

	//convert and calculate sum by column
	sum := [256]int{}
	for key, value := range weight {
		binaryWord := ToBinary(GetMD5Hash(key))

		
		for i := 0; i < len(binaryWord); i++ {
			if binaryWord[i] == '0' {
				sum[i] += -1 * value
			} else {
				sum[i] += value
			}
		}
	}
	//fingerprint
	fingerprint := [256]int{}
	for i, value := range sum {
		if value > 0 {
			fingerprint[i] = 1
		} else {
			fingerprint[i] = 0
		}
	}

	return fingerprint
}

func HammingDifference(file1 [256]int, file2 [256]int) (int) {
	counter := 0

	for i:=0; i < len(file1); i++ {
		if file1[i] != file2[i] {
			counter += 1
		}
	}

	return counter
}