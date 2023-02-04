package structures

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

type HLL struct {
	m   uint64
	p   uint8
	Reg []uint8
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func NewHll(p uint8) *HLL {

	m := math.Pow(2, float64(p))

	arr := make([]uint8, int(m))

	hll := HLL{m: uint64(m), p: p, Reg: arr}

	return &hll

}

func (hll *HLL) Put(s string) {

	bin := hash(s)

	bucket := bin >> (32 - hll.p)

	// counter := uint8(0)

	// for i := 0; i < int(32-hll.p); i++ {

	// 	if (bin & 1) == 1 {

	// 		break

	// 	}
	// 	counter += 1
	// 	bin = bin >> 1

	// }

	c := bits.TrailingZeros64(uint64(bin)) + 1

	if uint8(c) > hll.Reg[bucket] {

		hll.Reg[bucket] = uint8(c)

	}

}

func (hll HLL) print() {
	for i := 0; i < len(hll.Reg); i++ {

		fmt.Println(hll.Reg[i])

	}
}

func (hll *HLL) Encode() []byte {

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, hll.p)

	for i := 0; uint64(i) < hll.m; i++ {

		binary.Write(&buffer, binary.LittleEndian, hll.Reg[i])

	}

	return buffer.Bytes()
}

func (HLL) Decode(buffer bytes.Buffer) *HLL {

	hll := NewHll(0)

	err := binary.Read(&buffer, binary.LittleEndian, &hll.p)
	if err != nil {
		return nil
	}

	hll.m = uint64(math.Pow(2, float64(hll.p)))

	reg := make([]uint8, int(hll.m))

	for i := 0; uint64(i) < hll.m; i++ {

		binary.Read(&buffer, binary.LittleEndian, &reg[i])

	}
	hll.Reg = reg

	return hll
}
