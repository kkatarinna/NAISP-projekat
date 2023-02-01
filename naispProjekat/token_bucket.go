package main

import (
	"fmt"
	"time"
)

const (
	TIME_INTERVAL = 30
	TOKENS_NUMBER = 3
)

type tokenBucket struct {
	time int64
	tokens int64
}

func main() {

	//init first bucket, will be done from main
	timestamp := time.Now().Unix()
	tokenbucket := &tokenBucket{time: timestamp, tokens: TOKENS_NUMBER}

	for {
		var input string
		fmt.Println("1. send\n2. exit: ")
		fmt.Scanln(&input)
		if input == "1"{
			success := checkTokenBucket(tokenbucket)
			fmt.Println(success)
		} else if input == "2" {
			break
		}
	}
	
}

func checkTokenBucket(tbucket *tokenBucket) (bool) {
	now := time.Now().Unix()
	difference := now - tbucket.time

	if difference < TIME_INTERVAL {
		if tbucket.tokens > 0 {
			tbucket.tokens = tbucket.tokens - 1
			return true
		}
		return false
	} else {
		tbucket.time = now
		tbucket.tokens = TOKENS_NUMBER - 1

		return true
	}

}