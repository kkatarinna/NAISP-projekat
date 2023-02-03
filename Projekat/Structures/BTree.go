package structures

import (
	"fmt"
	. "projekat/Structures/SSTable"
	"sort"
	"time"
)

//m koliko dece moze da ima node
type BTree struct {
	root *Node
	m int
}


type Node struct {
	datas []Data
	next []*Node
}

type Data struct{
	tombstone bool
	timestamp uint64
	key string
	value []byte
}

//samo inicijalizuje merkleroot potrebno je pozvati FormMerkleTree
//da bi drvo bilo zapravo korisno
func CreateBTree(m int) *BTree{
	BTree := BTree{root:nil,m:m}
	return &BTree
}

//kljuc ne sme biti manji od 1
//potrebno proslediti adresu drveta u koje treba ubaciti
//vrati true/false ako je uspesno/neuspesno
func (t BTree) AddRecord(tree *BTree,rec Record) bool{
	if(tree.root == nil){
		data := Data{key:rec.Key,value:rec.Value,tombstone:rec.Tombstone,timestamp:rec.Timestamp}
		node := Node{datas:[]Data{data},next:[]*Node{nil,nil}}
		tree.root = &node
		return true
	}
	found,_,node,parent := tree.Find(rec.Key)
	if(found){
		return false
	}

	//Slucaj samo ubaci na prazno mesto
	if(len(node.datas) < tree.m - 1){
		data := Data{key:rec.Key,value:rec.Value,tombstone:rec.Tombstone,timestamp:rec.Timestamp}
		node.datas = append(node.datas, data)
		node.next = append(node.next, nil)
		sort.Slice(node.datas,func(i,j int) bool{
			return node.datas[i].key < node.datas[j].key
		})
		return true
	}

	//ako je prekoracio u korenu stabla
	if(node == tree.root){
		//naci srednji element
		node.datas = append(node.datas, Data{key:rec.Key,value:rec.Value,tombstone:rec.Tombstone,timestamp:rec.Timestamp})
		sort.Slice(node.datas,func(i,j int) bool{
			return node.datas[i].key < node.datas[j].key
		})

		middle := node.datas[len(node.datas)/2]

		//podaci pre srednjeg elementa
		d1 := node.datas[:(len(node.datas)/2)]
		n1 := Node{datas:d1,next:make([]*Node,len(d1)+1)}

		//podaci nakon srednjeg elementa
		d2 := node.datas[(len(node.datas)/2)+1:]
		n2 := Node{datas:d2,next:make([]*Node,len(d2)+1)}
		
		newRoot := Node{datas:[]Data{middle},next:[]*Node{&n1,&n2}}
		tree.root = &newRoot
		return true
	}

	//Slucaj Rotacija
	for i := range parent.next{
		if(parent.next[i] == node){

			//rotacije u levo
			if(i-1 >= 0){
				rotatedLeft := false
				if(len(parent.next[i-1].datas) < tree.m -1){
					parent.next[i-1].datas = append(parent.next[i-1].datas, parent.datas[i-1])
					parent.next[i-1].next = append(parent.next[i-1].next, nil)
					sort.Slice(parent.next[i-1].datas,func(n,m int)bool{
						return parent.next[i-1].datas[n].key < parent.next[i-1].datas[m].key
					})
					rotatedLeft = true
				}
				if(rotatedLeft){
					minKey := ""
					minValue := []byte{'0'}
					if(node.datas[0].key > rec.Key){
						minKey = rec.Key
						minValue = rec.Value
					}else{
						minKey = node.datas[0].key
						minValue = node.datas[0].value
						node.datas[0].key = rec.Key
						node.datas[0].value = rec.Value
						sort.Slice(node.datas,func(i,j int)bool{
							return node.datas[i].key < node.datas[j].key
						})
					}
					if(minKey == ""){
						panic("greska pri rotiranju u levo")
					}
					parent.datas[i-1].key = minKey
					parent.datas[i-1].value = minValue 
					return true
				}
			}

			//rotacija u desno
			if(i+1 < len(parent.next)){
				if(parent.next[i+1] != nil){
					rotatedRight := false
					if(len(parent.next[i+1].datas) < tree.m -1){
						parent.next[i+1].datas = append(parent.next[i+1].datas, parent.datas[i])
						parent.next[i+1].next = append(parent.next[i+1].next, nil)
						sort.Slice(parent.next[i+1].datas,func(n,m int)bool{
							return parent.next[i+1].datas[n].key < parent.next[i+1].datas[m].key
						})
						rotatedRight = true
					}
					if(rotatedRight){
						maxKey := ""
						maxValue := []byte{'0'}
						if(node.datas[len(node.datas)-1].key < rec.Key){
							maxKey = rec.Key
							maxValue = rec.Value
						}else{
							maxKey = node.datas[len(node.datas)-1].key
							maxValue = node.datas[len(node.datas)-1].value
							node.datas[len(node.datas)-1].key = rec.Key
							node.datas[len(node.datas)-1].value = rec.Value
							sort.Slice(node.datas,func(i,j int)bool{
								return node.datas[i].key < node.datas[j].key
							})
						}
						if(maxKey == ""){
							panic("greska pri rotiranju u levo")
						}
						parent.datas[i].key = maxKey
						parent.datas[i].value = maxValue 
						return true
					}
				}
			}
		}
	}

	//slucaj Podela Cvorova bez overflow
	for{
		//naci srednji element
		temp := make([]Data,len(node.datas))
		for i := range temp{
			temp[i] = node.datas[i]
		}
		timestamp := time.Now().Unix()
		temp = append(temp, Data{key:rec.Key,value:rec.Value,tombstone:false,timestamp:uint64(timestamp)})
		sort.Slice(temp,func(i,j int) bool{
			return temp[i].key < temp[j].key
		})
		middle := temp[len(node.datas)/2]
		n1 := temp[:(len(node.datas)/2)]
		n2 := temp[(len(node.datas)/2)+1:]

		next1 := node.next[:(len(node.next)/2)]
		if(len(next1) == len(n1)){
			next1 = append(next1, nil)
		}
		next2 := node.next[(len(node.next)/2):]
		node1 := Node{datas:n1,next:next1}
		node2 := Node{datas:n2,next:next2}
		newDatas := make([]Data,len(parent.datas)+1)
		for k := range parent.datas{
			newDatas[k] = parent.datas[k]
		}
		newDatas[len(newDatas) -1] = middle
		parent.datas = newDatas
		sort.Slice(parent.datas,func(i,j int) bool{
			return parent.datas[i].key < parent.datas[j].key
		})
		//da se pronadje node koji se cepa
		for i := range parent.next{
			if(parent.next[i] == node){
				parent.next[i] = &node1
				tmp2 := &node2
				if(i+1 < len(parent.next)){
					tmp := parent.next[i+1]
					parent.next[i+1] = tmp2
					tmp2 = tmp
					for j :=i+2; j < len(parent.next);j++{
						tmp = parent.next[j]
						parent.next[j] = tmp2
						tmp2 = tmp
					}
				}
				newNodes := make([]*Node,len(parent.next)+1)
				for k := range parent.next{
					newNodes[k] = parent.next[k]
				}
				newNodes[len(newNodes) -1] = tmp2
				parent.next = newNodes
			}
		}
		//da li je roditelj pretrpan?
		if(len(parent.datas) < tree.m){
			return true
		}else{
			for{
				_,_,n,p := tree.Find(parent.datas[0].key)
				node = n
				parent = p
				if(n == tree.root){
					//naci srednji element
				
					middle := p.datas[len(node.datas)/2]
				
					//podaci pre srednjeg elementa
					d1 := p.datas[:(len(node.datas)/2)]
					next1 := p.next[:(len(node.next)/2)]
					n1 := Node{datas:d1,next:next1}
				
					//podaci nakon srednjeg elementa
					d2 := p.datas[(len(node.datas)/2)+1:]
					next2 := p.next[(len(node.next)/2):]
					n2 := Node{datas:d2,next:next2}

					newRoot := Node{datas:[]Data{middle},next:[]*Node{&n1,&n2}}
					tree.root = &newRoot
					return true
				}else{
					//naci srednji element
				
					middle := n.datas[len(node.datas)/2]
				
					//podaci pre srednjeg elementa
					d1 := n.datas[:(len(node.datas)/2)]
					next1 := n.next[:(len(node.next)/2)]
					node1 := Node{datas:d1,next:next1}
				
					//podaci nakon srednjeg elementa
					d2 := n.datas[(len(node.datas)/2)+1:]
					next2 := n.next[(len(node.next)/2):]
					node2 := Node{datas:d2,next:next2}

					newDatas := make([]Data,len(parent.datas)+1)
					for k := range parent.datas{
						newDatas[k] = parent.datas[k]
					}
					newDatas[len(newDatas) -1] = middle
					parent.datas = newDatas
					sort.Slice(parent.datas,func(i,j int) bool{
						return parent.datas[i].key < parent.datas[j].key
					})

					//da se pronadje node koji se cepa
					for i := range parent.next{
						if(parent.next[i] == node){
							parent.next[i] = &node1
							tmp2 := &node2
							if(i+1 < len(parent.next)){
								tmp := parent.next[i+1]
								parent.next[i+1] = tmp2
								tmp2 = tmp
								for j :=i+2; j < len(parent.next);j++{
									tmp = parent.next[j]
									parent.next[j] = tmp2
									tmp2 = tmp
								}
							}
							newNodes := make([]*Node,len(parent.next)+1)
							for k := range parent.next{
								newNodes[k] = parent.next[k]
							}
							newNodes[len(newNodes) -1] = tmp2
							parent.next = newNodes
						}
					}
					//da li je roditelj pretrpan?
					if(len(parent.datas) < tree.m){
						return true
					}
				}
			}
		}
	}
}


//kljuc ne sme biti manji od 1
//potrebno proslediti adresu drveta u koje treba ubaciti
//vrati true/false ako je uspesno/neuspesno
func (t BTree)Add(tree *BTree,key string,d []byte) bool{
	if(tree.root == nil){
		timestamp := time.Now().Unix()
		data := Data{key:key,value:d,tombstone:false,timestamp:uint64(timestamp)}
		node := Node{datas:[]Data{data},next:[]*Node{nil,nil}}
		tree.root = &node
		return true
	}
	found,_,node,parent := tree.Find(key)
	if(found){
		return false
	}

	//Slucaj samo ubaci na prazno mesto
	if(len(node.datas) < tree.m - 1){
		timestamp := time.Now().Unix()
		data := Data{key:key,value:d,tombstone:false,timestamp:uint64(timestamp)}
		node.datas = append(node.datas, data)
		node.next = append(node.next, nil)
		sort.Slice(node.datas,func(i,j int) bool{
			return node.datas[i].key < node.datas[j].key
		})
		return true
	}

	//ako je prekoracio u korenu stabla
	if(node == tree.root){
		//naci srednji element
		timestamp := time.Now().Unix()
		node.datas = append(node.datas, Data{key:key,value:d,tombstone:false,timestamp:uint64(timestamp)})
		sort.Slice(node.datas,func(i,j int) bool{
			return node.datas[i].key < node.datas[j].key
		})

		middle := node.datas[len(node.datas)/2]

		//podaci pre srednjeg elementa
		d1 := node.datas[:(len(node.datas)/2)]
		n1 := Node{datas:d1,next:make([]*Node,len(d1)+1)}

		//podaci nakon srednjeg elementa
		d2 := node.datas[(len(node.datas)/2)+1:]
		n2 := Node{datas:d2,next:make([]*Node,len(d2)+1)}
		
		newRoot := Node{datas:[]Data{middle},next:[]*Node{&n1,&n2}}
		tree.root = &newRoot
		return true
	}

	//Slucaj Rotacija
	for i := range parent.next{
		if(parent.next[i] == node){

			//rotacije u levo
			if(i-1 >= 0){
				rotatedLeft := false
				if(len(parent.next[i-1].datas) < tree.m -1){
					parent.next[i-1].datas = append(parent.next[i-1].datas, parent.datas[i-1])
					parent.next[i-1].next = append(parent.next[i-1].next, nil)
					sort.Slice(parent.next[i-1].datas,func(n,m int)bool{
						return parent.next[i-1].datas[n].key < parent.next[i-1].datas[m].key
					})
					rotatedLeft = true
				}
				if(rotatedLeft){
					minKey := ""
					minValue := []byte{'0'}
					if(node.datas[0].key > key){
						minKey = key
						minValue = d
					}else{
						minKey = node.datas[0].key
						minValue = node.datas[0].value
						node.datas[0].key = key
						node.datas[0].value = d
						sort.Slice(node.datas,func(i,j int)bool{
							return node.datas[i].key < node.datas[j].key
						})
					}
					if(minKey == ""){
						panic("greska pri rotiranju u levo")
					}
					parent.datas[i-1].key = minKey
					parent.datas[i-1].value = minValue 
					return true
				}
			}

			//rotacija u desno
			if(i+1 < len(parent.next)){
				if(parent.next[i+1] != nil){
					rotatedRight := false
					if(len(parent.next[i+1].datas) < tree.m -1){
						parent.next[i+1].datas = append(parent.next[i+1].datas, parent.datas[i])
						parent.next[i+1].next = append(parent.next[i+1].next, nil)
						sort.Slice(parent.next[i+1].datas,func(n,m int)bool{
							return parent.next[i+1].datas[n].key < parent.next[i+1].datas[m].key
						})
						rotatedRight = true
					}
					if(rotatedRight){
						maxKey := ""
						maxValue := []byte{'0'}
						if(node.datas[len(node.datas)-1].key < key){
							maxKey = key
							maxValue = d
						}else{
							maxKey = node.datas[len(node.datas)-1].key
							maxValue = node.datas[len(node.datas)-1].value
							node.datas[len(node.datas)-1].key = key
							node.datas[len(node.datas)-1].value = d
							sort.Slice(node.datas,func(i,j int)bool{
								return node.datas[i].key < node.datas[j].key
							})
						}
						if(maxKey == ""){
							panic("greska pri rotiranju u levo")
						}
						parent.datas[i].key = maxKey
						parent.datas[i].value = maxValue 
						return true
					}
				}
			}
		}
	}

	//slucaj Podela Cvorova bez overflow
	for{
		//naci srednji element
		temp := make([]Data,len(node.datas))
		for i := range temp{
			temp[i] = node.datas[i]
		}
		timestamp := time.Now().Unix()
		temp = append(temp, Data{key:key,value:d,tombstone:false,timestamp:uint64(timestamp)})
		sort.Slice(temp,func(i,j int) bool{
			return temp[i].key < temp[j].key
		})
		middle := temp[len(node.datas)/2]
		n1 := temp[:(len(node.datas)/2)]
		n2 := temp[(len(node.datas)/2)+1:]

		next1 := node.next[:(len(node.next)/2)]
		if(len(next1) == len(n1)){
			next1 = append(next1, nil)
		}
		next2 := node.next[(len(node.next)/2):]
		node1 := Node{datas:n1,next:next1}
		node2 := Node{datas:n2,next:next2}
		newDatas := make([]Data,len(parent.datas)+1)
		for k := range parent.datas{
			newDatas[k] = parent.datas[k]
		}
		newDatas[len(newDatas) -1] = middle
		parent.datas = newDatas
		sort.Slice(parent.datas,func(i,j int) bool{
			return parent.datas[i].key < parent.datas[j].key
		})
		//da se pronadje node koji se cepa
		for i := range parent.next{
			if(parent.next[i] == node){
				parent.next[i] = &node1
				tmp2 := &node2
				if(i+1 < len(parent.next)){
					tmp := parent.next[i+1]
					parent.next[i+1] = tmp2
					tmp2 = tmp
					for j :=i+2; j < len(parent.next);j++{
						tmp = parent.next[j]
						parent.next[j] = tmp2
						tmp2 = tmp
					}
				}
				newNodes := make([]*Node,len(parent.next)+1)
				for k := range parent.next{
					newNodes[k] = parent.next[k]
				}
				newNodes[len(newNodes) -1] = tmp2
				parent.next = newNodes
			}
		}
		//da li je roditelj pretrpan?
		if(len(parent.datas) < tree.m){
			return true
		}else{
			for{
				_,_,n,p := tree.Find(parent.datas[0].key)
				node = n
				parent = p
				if(n == tree.root){
					//naci srednji element
				
					middle := p.datas[len(node.datas)/2]
				
					//podaci pre srednjeg elementa
					d1 := p.datas[:(len(node.datas)/2)]
					next1 := p.next[:(len(node.next)/2)]
					n1 := Node{datas:d1,next:next1}
				
					//podaci nakon srednjeg elementa
					d2 := p.datas[(len(node.datas)/2)+1:]
					next2 := p.next[(len(node.next)/2):]
					n2 := Node{datas:d2,next:next2}

					newRoot := Node{datas:[]Data{middle},next:[]*Node{&n1,&n2}}
					tree.root = &newRoot
					return true
				}else{
					//naci srednji element
				
					middle := n.datas[len(node.datas)/2]
				
					//podaci pre srednjeg elementa
					d1 := n.datas[:(len(node.datas)/2)]
					next1 := n.next[:(len(node.next)/2)]
					node1 := Node{datas:d1,next:next1}
				
					//podaci nakon srednjeg elementa
					d2 := n.datas[(len(node.datas)/2)+1:]
					next2 := n.next[(len(node.next)/2):]
					node2 := Node{datas:d2,next:next2}

					newDatas := make([]Data,len(parent.datas)+1)
					for k := range parent.datas{
						newDatas[k] = parent.datas[k]
					}
					newDatas[len(newDatas) -1] = middle
					parent.datas = newDatas
					sort.Slice(parent.datas,func(i,j int) bool{
						return parent.datas[i].key < parent.datas[j].key
					})

					//da se pronadje node koji se cepa
					for i := range parent.next{
						if(parent.next[i] == node){
							parent.next[i] = &node1
							tmp2 := &node2
							if(i+1 < len(parent.next)){
								tmp := parent.next[i+1]
								parent.next[i+1] = tmp2
								tmp2 = tmp
								for j :=i+2; j < len(parent.next);j++{
									tmp = parent.next[j]
									parent.next[j] = tmp2
									tmp2 = tmp
								}
							}
							newNodes := make([]*Node,len(parent.next)+1)
							for k := range parent.next{
								newNodes[k] = parent.next[k]
							}
							newNodes[len(newNodes) -1] = tmp2
							parent.next = newNodes
						}
					}
					//da li je roditelj pretrpan?
					if(len(parent.datas) < tree.m){
						return true
					}
				}
			}
		}
	}
}

//vraca bool da li je kljuc pronadjen
//i []byte vrednost koja se nalazi pod tim kljucem
//i vraca pokazivac na node gde se nalazi podatak(koristi meni za dodavanje)
//ako ne nadje vratice false i []byte{'o'} 
//posto nmp sta bi bila neka default vrednost za bajt
func (tree BTree)Find(key string) (bool,[]byte,*Node,*Node){
	if(tree.root == nil){
		return false, []byte{'0'},nil,nil
	}
	here := tree.root
	before := here
	parent := before
	for{
		before = here
		i := 0
		for{
			if(key < here.datas[i].key){
				here = here.next[i]
				break
			}
			if(here.datas[i].key == key){
				return true, here.datas[i].value,here,parent
			}
			i++
			if(i == len(here.datas)){
				here = here.next[i]
				break
			}
		}
		if(here == nil){
			return false, []byte{'0'},before,parent
		}
		parent = before
	}
}

func PrintNode(k int,node *Node){
	fmt.Println("node: ", k)
	for _,data := range node.datas{
		fmt.Println("key: ",data.key,", value: ",data.value,", tombstone: ",data.tombstone,", timestamp: ",data.timestamp)
	}
	fmt.Println()
}

func (tree BTree) PrintBTreeWidth(){
	h := make([]*Node,0)
	h = append(h,tree.root)
	h2 := make([]*Node,0)
	i:= 0
	for{
		fmt.Println()
		fmt.Println("nivo:: ", i)
		for k,n := range h{
			if(n == nil){
				return
			}
			PrintNode(k,n)
			for j := 0 ; j <len(n.next);j++{
				h2 = append(h2,n.next[j])
			}
		}
		i++
		h = h2
		h2 = make([]*Node,0)
	}
}

func (tree BTree) LogicDelete(key string) bool{
	found,_,here,_ := tree.Find(key)
	if(found == false){
		return false
	}
	for i:= range here.datas{
		if(here.datas[i].key == key){
			here.datas[i].tombstone = true
			fmt.Println("izbrisan je element")
			return true
		}
	}
	return false
}

func (tree BTree) GetAll() *[]*Data{
	h := make([]*Node,0)
	h = append(h,tree.root)
	h2 := make([]*Node,0)
	data := make([]*Data,0)
	i:= 0
	for{
		for _,n := range h{
			if(n == nil){
				return &data
			}
			for j := 0 ; j <len(n.next);j++{
				h2 = append(h2,n.next[j])
				for _,d := range n.datas{
					data = append(data, &d)
				}
			}
		}
		i++
		h = h2
		h2 = make([]*Node,0)
	}
}
//func main(){
//	tree := CreateBTree(3)
//	tree.Add(&tree,"A",[]byte{'n'})
//	tree.Add(&tree,"DA",[]byte{'n'})
//	tree.Add(&tree,"BD",[]byte{'n'})
//	tree.Add(&tree,"B",[]byte{'n'})
//	tree.Add(&tree,"AD",[]byte{'n'})
//	tree.Add(&tree,"AA",[]byte{'n'})
//	tree.Add(&tree,"BA",[]byte{'n'})
//	tree.Add(&tree,"CC",[]byte{'n'})
//	tree.Add(&tree,"C",[]byte{'n'})
//	tree.Add(&tree,"DC",[]byte{'n'})
//	tree.Add(&tree,"BC",[]byte{'n'})
//	tree.Add(&tree,"DD",[]byte{'n'})
//	tree.Add(&tree,"BB",[]byte{'n'})
//	tree.Add(&tree,"D",[]byte{'n'})
//	tree.Add(&tree,"AB",[]byte{'n'})
//	tree.Add(&tree,"AC",[]byte{'n'})
//	tree.Add(&tree,"CB",[]byte{'n'})
//	tree.Add(&tree,"CD",[]byte{'n'})
//	tree.Add(&tree,"CA",[]byte{'n'})
//	tree.Add(&tree,"DB",[]byte{'n'})
//	tree.PrintBTreeWidth()
//}