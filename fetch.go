package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	//"errors"
	"container/list"
	"strconv"
	"sync"

	"github.com/reiver/go-porterstemmer"
)

var seperators []byte = []byte{10, 32, 44, 46}
var stopWords map[string]bool

//var dictionary map[string]*list.List
var groupLen int = 50

var dictionary = struct {
	sync.RWMutex
	m map[string]*list.List
}{m: make(map[string]*list.List)}

func main() {
	var corpusPath string
	flag.StringVar(&corpusPath, "corpus", "data/corpus", "File path of the corpus.")
	flag.Parse()
	dispatcher(corpusPath)
}

func dispatcher(corpusPath string) {
	file, err := os.Open(corpusPath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	collectStopWords()
	//dictionary = make(map[string]*list.List)
	scanner := bufio.NewScanner(io.Reader(file))

	scanner.Split(splitTokens)

	var (
		token string
		pos   uint32 = 0
		wg    sync.WaitGroup
	)
	for scanner.Scan() {
		token = scanner.Text()
		if len(token) > 0 {
			wg.Add(1)
			go addToken(token, pos, &wg)
			pos += 1
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	wg.Wait()

	outFile, err := os.Create("res")
	defer outFile.Close()
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(io.Writer(outFile))
	writeIndex(writer)
}

func addToken(token string, pos uint32, wg *sync.WaitGroup) {
	defer wg.Done()

	// Check to see if token is a stop word
	_, ok := stopWords[token]
	if !ok {
		// Stem the token
		token = porterstemmer.StemString(token)
		dictionary.RLock()
		postingsList, ok := dictionary.m[token]
		dictionary.RUnlock()
		if ok {
			lastEl := postingsList.Back()
			lastGroup := lastEl.Value.([]uint32)
			if len(lastGroup) == groupLen {
				newGroup := []uint32{pos}
				postingsList.PushBack(newGroup)
			} else {
				lastGroup = append(lastGroup, pos)
				postingsList.Remove(lastEl)
				postingsList.PushBack(lastGroup)
			}
		} else {
			l := list.New()
			newGroup := []uint32{pos}
			l.PushBack(newGroup)
			dictionary.Lock()
			dictionary.m[token] = l
			dictionary.Unlock()
		}
	}
}

func writeIndex(writer *bufio.Writer) {
	for k, v := range dictionary.m {
		fmt.Println(k, v.Len(), len(v.Front().Value.([]uint32)))
		writer.WriteString("#" + k)
		var group []uint32
		for el := v.Front(); el != nil; el = el.Next() {
			group = el.Value.([]uint32)
			for _, posting := range group {
				val := strconv.FormatUint(uint64(posting), 10)
				writer.WriteString("," + string(val))
			}
		}
	}
	writer.Flush()
}

func splitTokens(data []byte, atEOF bool) (advance int, token []byte, err error) {
	var (
		found   = false
		started = false
		offset  = 0
	)
	for i := 0; i < len(data); i++ {
		found = false
		for s := 0; s < len(seperators); s++ {
			if data[i] == seperators[s] {
				advance = i + 1
				token = data[offset:i]
				if !started {
					offset += 1
				}
				found = true
				break
			}
		}
		if found {
			if started {
				break
			} else {
				continue
			}
		}
		if !started {
			started = true
		}
	}
	return
}

func collectStopWords() {
	file, err := os.Open("data/stopwords")
	defer file.Close()
	if err != nil {
		panic(err)
	}
	stopWords = make(map[string]bool)
	scanner := bufio.NewScanner(io.Reader(file))

	var word string
	for scanner.Scan() {
		word = scanner.Text()
		if len(word) > 0 {
			stopWords[word] = true
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
