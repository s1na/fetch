package main

import (
	//"fmt"
	"testing"
)

func TestSmallCorpus(t *testing.T) {
	CreateIndex("data/corpus28")
}

/*func TestSlice(t *testing.T) {
	a := make([]uint32, 10, 10)
	fmt.Println(len(a), cap(a))
	a[0] = 1231
	fmt.Println(a)
}*/
/*func TestConv(t *testing.T) {
	var fac float32 = 1.2

	var cur int = 40
	for i := 0; i < 100; i++ {
		cur = int(float32(cur) * fac)
		fmt.Println(cur)
	}
}*/

func BenchmarkAllocLow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			t := make([]uint32, 1)
			t[0] = 13
		}
	}
}

func BenchmarkAllocMid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			t := make([]uint32, 10)
			t[0] = 13
		}
	}
}

func BenchmarkAllocHigh(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 700000; j++ {
			t := make([]uint32, 100)
			t[0] = 13
		}
	}
}
