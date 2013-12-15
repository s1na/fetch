package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	//"errors"
	"container/list"
	//"runtime"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/reiver/go-porterstemmer"
)

var seperators = [6]byte{10, 32, 34, 44, 46, 59}
var stopWords map[string]bool
var groupLen int = 50
var dictionary = struct {
	sync.RWMutex
	m map[string]*list.List
	keys []string
}{m: make(map[string]*list.List)}
var notStem = map[string]bool{
	"ION": true,
}
var indexMemoryLimit uint32 = 512 * 1024 * 1024
var indexMemoryConsumed = struct {
	sync.RWMutex
	v uint32
}{v: 0}

func main() {
	var corpusPath string
	flag.StringVar(&corpusPath, "corpus", "data/corpus", "File path of the corpus.")
	flag.Parse()

	fmt.Println("Creating index.")
	dispatcher(corpusPath)
}

func dispatcher(corpusPath string) {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	file, err := os.Open(corpusPath)
	defer file.Close()
	if err != nil {
		panic(err)
	}

	collectStopWords()
	scanner := bufio.NewScanner(io.Reader(file))
	scanner.Split(splitTokens)

	var (
		token string
		pos   uint32 = 0
	)
	for scanner.Scan() {
		token = scanner.Text()
		if len(token) > 0 {
			addToken(token, pos)
			pos += 1
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	outFile, err := os.Create("res")
	defer outFile.Close()
	if err != nil {
		panic(err)
	}
	sort.Strings(dictionary.keys)
	writeIndex(io.Writer(outFile))
}

func addToken(token string, pos uint32) {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Println(err)
			fmt.Println(token)
			os.Exit(1)
		}
	}()

	// Check to see if token is a stop word
	var memoryConsumed uint32 = 0
	if _, ok := stopWords[token]; !ok {
		// Stem the token
		if _, ok := notStem[token]; !ok {
			token = porterstemmer.StemString(token)
		}
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
			dictionary.keys = append(dictionary.keys, token)
			//fmt.Println(token, uint32(unsafe.Sizeof(token)))
			memoryConsumed += uint32(unsafe.Sizeof(token))
		}
	}
	indexMemoryConsumed.Lock()
	indexMemoryConsumed.v += memoryConsumed
	indexMemoryConsumed.Unlock()
}

func writeIndex(w io.Writer) {
	writer := bufio.NewWriter(w)
	var v *list.List
	for _, k := range dictionary.keys {
		v = dictionary.m[k]
		//fmt.Println(k, v.Len(), len(v.Front().Value.([]uint32)))
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
