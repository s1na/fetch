package main

import (
	"bytes"
	"bufio"
	"encoding/binary"
	"container/heap"
	"fmt"
	"math"
	"io"
	"os"
	"strings"
	//"runtime/pprof"

	"github.com/s1na/go-porterstemmer"
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

func retrieve(q string, phrase bool) []*Result {
	/*pf, err := os.Create("retcpu.prof")
	defer pf.Close()
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(io.Writer(pf))
	pprof.StartCPUProfile(writer)
*/

	qStrings := strings.Split(q, " ")
	var qTerms []*Term//= make([]*Term, len(qStrings))
	var matchingTerms []*Term
	var t *Term
	var ts string
	for i := 0; i < len(qStrings); i++ {
		ts = qStrings[i]
		ts = strings.ToLower(ts)
		ts = porterstemmer.StemString(ts)
		if !isStopWord(ts) {
			if strings.HasSuffix(ts, "%2a") {
				matchingTerms = getMatchingTerms(ts)
				if matchingTerms != nil {
					qTerms = append(qTerms, matchingTerms...)
				}
			} else {
				t = getTerm(ts)
				if t != nil {
					qTerms = append(qTerms, t)
				}
			}
		}
	}
	var results []*Result
	if len(qTerms) > 0 {
		if phrase {
			results = phraseSearch(qTerms, 8)
		} else {
			results = docAtATime(qTerms, 8)
		}
	}

	//pprof.StopCPUProfile()

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

func phraseSearch(q []*Term, k int) []*Result {
	var res []*Result
	/*for i := range len(q) {
		postingsList[i] = getPostingsList(q[i])
	}*/

	terms := &TermHeap{}
	heap.Init(terms)
	var nDoc *Document
	for i := 0; i < len(q); i++ {
		nDoc = nextDoc(q[i], 0)
		if nDoc != nil {
			heap.Push(terms, &TermNextDoc{term: q[i], nextDoc: nDoc})
		}
	}

	indexFile, err := os.Open("index/index.1")
	defer indexFile.Close()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(io.Reader(indexFile))

	postingsList := make([][]int, len(q))
	foundCoveringDoc := false
	eq := true
	var term *TermNextDoc
	var indices []int
	oneEnded := false
	for !oneEnded {
		foundCoveringDoc = false
		for !foundCoveringDoc {
			eq = true
			for i := 0; i < len(*terms); i++ {
				if (*terms)[i].nextDoc.docId != (*terms)[0].nextDoc.docId {
					eq = false
				}
			}
			if eq {
				foundCoveringDoc = true
				break
			}
			// Not all equal
			term = heap.Pop(terms).(*TermNextDoc)
			nDoc = nextDoc(term.term, term.nextDoc.docId)
			if nDoc != nil {
				heap.Push(terms, &TermNextDoc{term: term.term, nextDoc: nDoc})
			} else {
				oneEnded = true
				break
			}
		}
		if oneEnded {
			break
		}
		for i := 0; i < len(*terms); i++ {
			indexFile.Seek((*terms)[i].term.pos, 0)
			postingsList[i] = getPostingsList((*terms)[i].term, (*terms)[0].nextDoc.docId, reader)
			reader.Reset(io.Reader(indexFile))
		}
		indices = make([]int, len(*terms))
		nc := true
		for true {
			min := 0
			for j := 0; j < len(*terms) - 1; j++ {
				if postingsList[j][indices[j]] < postingsList[min][indices[min]] {
					min = j
				}
				if postingsList[j][indices[j]] != postingsList[j + 1][indices[j + 1]] - 1 {
					nc = false
					break
				}
			}
			//fmt.Println(len(postingsList), len(*terms) - 1)
			if len(*terms) > 1 {
				if postingsList[len(*terms) - 1][indices[len(*terms) - 1]] < min {
					min = len(*terms) - 1
				}
			} else if len(*terms) == 0 {
				oneEnded = true
				break
			}
			if nc {
				res = append(res, &Result{doc: (*terms)[0].nextDoc.docId, score: 1})
				term = heap.Pop(terms).(*TermNextDoc)
				nDoc = nextDoc(term.term, term.nextDoc.docId)
				if nDoc != nil {
					heap.Push(terms, &TermNextDoc{term: term.term, nextDoc: nDoc})
				}
				break
			}
			if len(postingsList[min]) == indices[min] + 1 {
				term = heap.Pop(terms).(*TermNextDoc)
				nDoc = nextDoc(term.term, term.nextDoc.docId)
				if nDoc != nil {
					heap.Push(terms, &TermNextDoc{term: term.term, nextDoc: nDoc})
				} else {
					oneEnded = true
				}
				break
			}
			indices[min]++
		}
	}
	upper := len(res)
	if k < upper {
		upper = k
	}
	return res[:upper]
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
	bottom := (1 - BM25B) + (BM25B * (float64(docInfos[d.docId-1].length) / docLenAvg))
	bottom = tfF + (BM25K1 * bottom)

	return top / bottom
}

func getTerm(t string) *Term {
	for i := 0; i < len(dict); i++ {
		if dict[i].term == t {
			return dict[i]
		}
	}
	return nil
}

func getMatchingTerms(p string) []*Term {
	res := make([]*Term, 0)
	p = strings.TrimSuffix(p, "%2a")
	for i := 0; i < len(dict); i++ {
		if strings.HasPrefix(dict[i].term, p) {
			fmt.Println(dict[i].term)
			res = append(res, dict[i])
		}
	}
	return res
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

func getPostingsList(t *Term, docId int, rd io.Reader) []int {
	var res []int
	buf := make([]byte, 4)
	var tmp uint64 = 0
	d := 0
	tf := 0
	num := 0
	foundDoc := false
	zero := []byte{0, 0, 0, 0}
	/*fmt.Println(d)
	fmt.Println(t.term)*/
	for !foundDoc {
		rd.Read(buf)
		tmp, _ = binary.Uvarint(buf)
		d = int(tmp)
		if bytes.Equal(buf, zero) {
			break
		}
		rd.Read(buf)
		tmp, _ = binary.Uvarint(buf)
		tf = int(tmp)
		if d == docId {
			foundDoc = true
			for i := 0; i < tf; i++ {
				rd.Read(buf)
				tmp, _ := binary.Uvarint(buf)
				num = int(tmp)
				res = append(res, num)
			}
			break
		}
		for i := 0; i < tf; i++ {
			rd.Read(buf)
		}
	}
	if !foundDoc {
		panic("Didn't find doc in posting.")
	}
	return res
}

func getDocText(docId int) string {
	if docId == 0 {
		return string("")
	}
	file, err := os.Open(corpusPath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	buf := make([]byte, docInfos[docId].pos-docInfos[docId-1].pos)
	file.Seek(int64(docInfos[docId-1].pos), 0)
	file.Read(buf)
	return string(buf)
}

func getDocTitle(text string) string {
	start := strings.Index(text, "<TITLE>")
	if start != -1 {
		end := strings.Index(text, "</TITLE>")
		return text[start + 7: end]
	}
	return ""
}

func getDocBody(text string) string {
	start := strings.Index(text, "<BODY>")
	if start != -1 {
		end := strings.Index(text, "</BODY>")
		return text[start + 6: end]
	}
	return ""

}
