package main

import (
	"container/heap"
	"fmt"
	"math"
	"strings"
)

const (
	BM25K1 = 1.2
	BM25B  = 0.75
)

type Result struct {
	doc   int
	score float64
}

type TermNextDoc struct {
	term    *Term
	nextDoc *Document
}

func retrieve(q string) []*Result {
	qStrings := strings.Split(q, " ")
	var qTerms []*Term = make([]*Term, len(qStrings))
	for i := 0; i < len(qStrings); i++ {
		qTerms[i] = getTerm(qStrings[i])
	}
	results := docAtATime(qTerms, 10)
	for i := 0; i < len(results); i++ {
		fmt.Println(results[i].doc, results[i].score)
	}
	return results
}

func docAtATime(q []*Term, k int) []*Result {
	results := &ResultHeap{}
	terms := &TermHeap{}
	heap.Init(results)
	heap.Init(terms)
	for i := 0; i < k; i++ {
		heap.Push(results, &Result{0, 0})
	}
	for i := 0; i < len(q); i++ {
		nextDoc := nextDoc(q[i], 0)
		if nextDoc != nil {
			heap.Push(terms, &TermNextDoc{term: q[i], nextDoc: nextDoc})
		}
	}

	var term *TermNextDoc
	var d int
	var score float64
	var nDoc *Document
	var res *Result
	popped := false
	for len(*terms) > 0 {
		popped = false
		term = heap.Pop(terms).(*TermNextDoc)
		d = term.nextDoc.docId
		score = 0.0

		for d == term.nextDoc.docId {
			score += BM25(term.term, term.nextDoc)
			nDoc = nextDoc(term.term, d)
			if nDoc != nil {
				heap.Push(terms, &TermNextDoc{term: term.term, nextDoc: nDoc})
			}

			if len(*terms) > 0 {
				term = heap.Pop(terms).(*TermNextDoc)
				popped = true
			} else {
				break
			}
		}
		if popped {
			heap.Push(terms, term)
		}
		if score > 0.0 {
			res = &Result{doc: d, score: score}
			results.PushGreater(res)
		}
	}

	sortedResults := make([]*Result, (*results).Len())
	for i := len(sortedResults) - 1; i >= 0; i-- {
		sortedResults[i] = heap.Pop(results).(*Result)
	}
	return sortedResults
}

func BM25(t *Term, d *Document) float64 {
	termDocsF := float64(t.docs.Len())
	logRes := math.Log2(totalDocsF / termDocsF)
	res := logRes * TFBM25(t, d)
	return res
}

func TFBM25(t *Term, d *Document) float64 {
	tfF := float64(d.tf)
	top := tfF * (BM25K1 + 1)
	bottom := (1 - BM25B) + (BM25B * (float64(docLens[d.docId-1]) / docLenAvg))
	bottom = tfF + (BM25K1 * bottom)

	return top / bottom
}

func getTerm(t string) *Term {
	for i := 0; i < totalTerms; i++ {
		if dict[i].term == t {
			return dict[i]
		}
	}
	panic("No term found!")
}

func getDocument(t *Term, docId int) *Document {
	var doc *Document
	for el := t.docs.Front(); el != nil; el = el.Next() {
		doc = el.Value.(*Document)
		if doc.docId == docId {
			return doc
		}
	}
	return nil
}

func nextDoc(t *Term, currentDoc int) *Document {
	var doc *Document
	for el := t.docs.Front(); el != nil; el = el.Next() {
		doc = el.Value.(*Document)
		if doc.docId > currentDoc {
			return doc
		}
	}
	return nil
}

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
