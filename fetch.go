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
	Title string
	Body string
}

type ErrorResponse struct {
	Err bool
	Msg string
}

var stopWords map[string]bool
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

	collectStopWords()
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
	var results []*Result
	if r.FormValue("phrase") == "true" {
		results = retrieve(q, true)
	} else {
		results = retrieve(q, false)
	}
	
	if results != nil {
		resultResponses := make([]*ResultResponse, len(results))
		var text string
		var title string
		var body string
		notNil := false
		for i := 0; i < len(results); i++ {
			if results[i] == nil {
				continue
			}
			if results[i].doc == 0 && results[i].score == 0 {
				continue
			}
			notNil = true
			text = getDocText(results[i].doc)
			title = getDocTitle(text)
			body = getDocBody(text)
			resultResponses[i] = &ResultResponse{
				DocId: results[i].doc,
				Score: results[i].score,
				Text: getDocText(results[i].doc),
				Title: title,
				Body: body,
			}
		}
		/*fmt.Println(len(resultResponses))
		fmt.Println(resultResponses)*/
		if notNil {
			w.WriteJson(&resultResponses)
		} else {
			w.WriteJson(&ErrorResponse{Err: true, Msg: "No document found, please try another query."})
		}
	} else {
		w.WriteJson(&ErrorResponse{Err: true, Msg: "No document found, please try another query."})
	}
}
