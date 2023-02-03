package structures

import (
	"time"
)

const (
	TIME_INTERVAL = 30
	TOKENS_NUMBER = 3
)

type TokenBucket struct {
	Time int64
	Tokens int64
}

// func main() {

	// //init first bucket, will be done from main
	// timestamp := time.Now().Unix()
	// tokenbucket := &TokenBucket{Time: timestamp, Tokens: config.TokensNumber}

	// for {
	// 	var input string
	// 	fmt.Println("1. send\n2. exit: ")
	// 	fmt.Scanln(&input)
	// 	if input == "1"{
	// 		success := CheckTokenBucket(config, tokenbucket)
	// 		fmt.Println(success)
	// 	} else if input == "2" {
	// 		break
	// 	}
	// }
	
// }

func CheckTokenBucket(config *Config, tbucket *TokenBucket) (bool) {
	now := time.Now().Unix()
	difference := now - tbucket.Time

	if difference < config.TimeInterval {
		if tbucket.Tokens > 0 {
			tbucket.Tokens = tbucket.Tokens - 1
			return true
		}
		return false
	} else {
		tbucket.Time = now
		tbucket.Tokens = config.TokensNumber - 1

		return true
	}

}