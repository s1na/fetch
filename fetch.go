package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

type DictItem struct {
	term string
	pos	int64
}

var dict []*DictItem

func main() {
	var create bool
	var start bool
	var corpusPath string
	var indexPath string

	flag.BoolVar(&create, "create", false, "Create the indices.")
	flag.StringVar(&corpusPath, "corpus", "data/corpus", "File path of the corpus.")
	flag.BoolVar(&start, "start", true, "Start the service.")
	flag.StringVar(&indexPath, "index", "index/index", "File path of the index.")
	flag.Parse()

	if create {
		fmt.Println("Creating index.")
		createIndex(corpusPath)
	}
	if start {
		readIndex(indexPath)
		fmt.Println(dict[0], dict[1], dict[2])
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
