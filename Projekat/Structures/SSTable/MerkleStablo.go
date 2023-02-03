package sstable

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

type MerkleRoot struct {
	root *NodeMerkle
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type NodeMerkle struct {
	data  []byte
	left  *NodeMerkle
	right *NodeMerkle
}

type NodeSerialize struct {
	Data [20]byte
}

func (n *NodeMerkle) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// samo inicijalizuje merkleroot potrebno je pozvati FormMerkleTree
// da bi drvo bilo zapravo korisno
func CreateMerkleRoot() MerkleRoot {
	MerkleRoot := MerkleRoot{root: nil}
	return MerkleRoot
}

// pravljenje drveta od niza podataka []byte i serialize bool ako je potrebna serijalizacija stabla
// (uglavnom ce biti false samo kad se ucitava iz datoteke i ponovo formira stablo)
// svaki put kad se izmene podaci mora se formirati novo stablo
// korisno logicko brisanje onda kad je potreban update samo se formira novo
// sa sredjenim podacima
// VRACA DRVO STAVITE MR = MR.FormMerkleTree...
func (mr MerkleRoot) FormMerkleTree(filepath string, data [][]byte, serialize bool) MerkleRoot {
	nodes := make([]NodeMerkle, 0)
	for _, element := range data {
		d := Hash(element)
		leaf := NodeMerkle{data: d[:], left: nil, right: nil}
		nodes = append(nodes, leaf)
	}

	//serijalizacija listova
	//if serialize {
	//	Serialize(filepath, data)
	//}
	i := 0
	h1 := nodes

	for {
		h2 := make([]NodeMerkle, 0)
		if len(h1)%2 != 0 {
			h1 = append(h1, NodeMerkle{data: []byte{'0'}, left: nil, right: nil})
		}
		for {
			if i > len(h1)-1 {
				break
			}
			left := h1[i]
			right := h1[i+1]
			combine := append(left.data, right.data...)
			d := Hash(combine)
			node := NodeMerkle{data: d[:], left: &left, right: &right}
			h2 = append(h2, node)
			i += 2
		}
		i = 0
		h1 = h2
		if len(h1) == 1 {
			mr.root = &h1[0]
			mr.SerializeRoot(filepath)
			return mr
		}
	}

}

// hesira podatke data pravi serijalizabilne cvorove i stavlja u Merkle.bin
func Serialize(filepath string, data [][]byte) {
	data2 := []NodeSerialize{}
	for _, d := range data {
		value := Hash(d)
		n := NodeSerialize{Data: value}
		data2 = append(data2, n)
	}
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	binary.Write(file, binary.LittleEndian, data2)
	file.Close()
}

func (mr MerkleRoot)SerializeRoot(filepath string){
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	file.Write(mr.root.data)
	file.Close()
}
// ucitava samo lisce koristi za ucitavanje stabla
func LoadLeafs(filepath string) [][]byte {
	data2 := []NodeSerialize{}
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	newv := &NodeSerialize{}
	for {
		err = binary.Read(file, binary.LittleEndian, newv)
		if err != nil {
			break
		}
		data2 = append(data2, *newv)

	}
	file.Close()
	data := [][]byte{}
	for _, d := range data2 {
		data = append(data, d.Data[:])
	}
	return data
}

// ucitava merkle stablo i vraca MerkleRoot iz fajla Merkle.bin
func LoadMerkle(filepath string) MerkleRoot {
	data := LoadLeafs(filepath)
	mr := CreateMerkleRoot()
	mr = mr.FormMerkleTree(filepath, data, false)
	return mr
}

// ispis sa leva na desno (inOrder)
// treba mu prvi element na vrhu (merkle.root)
func PrintMerkleInOrder(myNode *NodeMerkle) {
	if myNode != nil {
		fmt.Printf("adress:%p, \nleft:%p, \nright:%p, \nvalue:%v\n\n", myNode, myNode.left, myNode.right, myNode.data)
		PrintMerkleInOrder(myNode.left)
		PrintMerkleInOrder(myNode.right)
	}
}

// ispis po nivoima
// treba mu (merkle.root)
func PrintMerkleWidth(myNode *NodeMerkle) {
	h := make([]*NodeMerkle, 0)
	h = append(h, myNode)
	h2 := make([]*NodeMerkle, 0)
	i := 0
	for {
		fmt.Println("nivo:: ", i)
		for _, n := range h {
			if n == nil {
				return
			}
			fmt.Printf("adress:%p, \nleft:%p, \nright:%p, \nhashed value:%v, \nvalue:%s\n\n", n, n.left, n.right, n.data, n.String())
			h2 = append(h2, n.left)
			h2 = append(h2, n.right)
		}
		i++
		h = h2
		h2 = make([]*NodeMerkle, 0)
	}
}

// func main() {
// 	mr := CreateMerkleRoot()
// 	value := []byte{'n', 'e', 's', 't', 'o'}
// 	nodes := [][]byte{value, value, value, value}
// 	mr = mr.FormMerkleTree(nodes, false)
// 	fmt.Println("PRVO DRVO::::: ")
// 	PrintMerkleWidth(mr.root)
// 	mr2 := LoadMerkle()
// 	fmt.Println("UCITANO DRVO::::: ")
// 	PrintMerkleWidth(mr2.root)
// }
