package main

import (
	"container/list"
	"flag"
	"fmt"
	"log"
	"net/http"
)

type Term struct {
	term string
	docs *list.List
	pos  int64
}

type Document struct {
	docId int
	tf    int
}

var dict []*Term
var totalDocs int = 0
var totalDocsF float64
var totalTerms int = 0
var docLenAvg float64 = 0
var docLens []int

func main() {
	var create bool
	var start bool
	var corpusPath string
	var indexPath string

	flag.BoolVar(&create, "create", false, "Create the indices.")
	flag.StringVar(&corpusPath, "corpus", "data/corpus", "File path of the corpus.")
	flag.BoolVar(&start, "start", true, "Start the service.")
	flag.StringVar(&indexPath, "index", "index/index.1", "File path of the index.")
	flag.Parse()

	if create {
		fmt.Println("Creating index.")
		createIndex(corpusPath)
	} else if start {
		readIndex(indexPath)
		readMetaData("index/metadata")
		totalDocsF = float64(totalDocs)
		fmt.Println("A total of", totalTerms, "terms and", totalDocs, "docs read from index.")

		retrieve("cocoa")
		startService()
	}
}

func startService() {
	http.Handle("/search", http.HandlerFunc(searchHandler))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(dict[0].term))
}
