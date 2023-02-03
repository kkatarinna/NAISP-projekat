package main

import "fmt"

func main() {
	var value = []byte{'n', 'e', 's', 't', 'o'}
	sl := CreateSkipList(5, 10)
	
	fmt.Println("DODAVANJE SVIH ELEMENATA::")
	fmt.Println()
	sl.Add("b", value)
	sl.Add("e", value)
	sl.Add("f", value)
	sl.Add("c", value)
	sl.Add("a", value)
	sl.print()

	fmt.Println("LOGICKO BRISANJE A::")
	fmt.Println()
	sl.logicDelete("a")
	sl.print()

	fmt.Println("DODAVANJE A::")
	fmt.Println()
	sl.Add("a",value)
	sl.print()

	fmt.Println("LOGICKO BRISANJE A::")
	fmt.Println()
	sl.logicDelete("a")
	sl.print()

	fmt.Println("FIZICKO BRISANJE A::")
	fmt.Println()
	sl.delete("a")
	sl.print()
}
