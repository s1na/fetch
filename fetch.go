package main

import (
	"container/list"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/ant0ine/go-json-rest"
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

type DocInfo struct {
	length int
	pos    int64
}

type ResultResponse struct {
	DocId int
	Score float64
	Text string
}

var corpusPath string
var dict []*Term
var totalDocs int = 0
var totalDocsF float64
var totalTerms int = 0
var docInfos []*DocInfo
var docLenAvg float64 = 0

func main() {
	var create bool
	var start bool
	var indexPath string

	flag.BoolVar(&create, "create", false, "Create the indices.")
	flag.StringVar(&corpusPath, "corpus", "data/corpus28", "File path of the corpus.")
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

		startService()
	}
}

func startService() {
	handler := rest.ResourceHandler{}
	handler.SetRoutes(
		rest.Route{"GET", "/search/:query", SearchHandler},
		rest.Route{"POST", "/search", SearchHandler},
	)
	log.Fatal(http.ListenAndServe(":8080", &handler))
}

func SearchHandler(w *rest.ResponseWriter, r *rest.Request) {
	q := r.PathParam("query")
	q = strings.Replace(q, ",", " ", -1)
	fmt.Println(q)
	results := retrieve(q)
	
	resultResponses := make([]*ResultResponse, len(results))
	for i := 0; i < len(results); i++ {
		resultResponses[i] = &ResultResponse{
			DocId: results[i].doc,
			Score: results[i].score,
			Text: getDocText(results[i].doc),
		}
	}
	fmt.Println(len(resultResponses))
	fmt.Println(resultResponses)
	w.WriteJson(&resultResponses)
}
