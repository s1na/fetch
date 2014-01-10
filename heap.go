package main

import (
	"container/heap"
)

type ResultHeap []*Result

func (h ResultHeap) Len() int {
	return len(h)
}

func (h ResultHeap) Less(i, j int) bool {
	return h[i].score < h[j].score
}

func (h ResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ResultHeap) Push(x interface{}) {
	*h = append(*h, x.(*Result))
}

func (h *ResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *ResultHeap) PushGreater(x interface{}) {
	old := *h
	if old[0].score < x.(*Result).score {
		heap.Pop(h)
		heap.Push(h, x)
	}
}

type TermHeap []*TermNextDoc

func (h TermHeap) Len() int {
	return len(h)
}

func (h TermHeap) Less(i, j int) bool {
	return h[i].nextDoc.docId < h[j].nextDoc.docId
}

func (h TermHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TermHeap) Push(x interface{}) {
	*h = append(*h, x.(*TermNextDoc))
}

func (h *TermHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
