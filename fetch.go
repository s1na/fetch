package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"container/list"
	"sort"
	"encoding/binary"
	"strings"
	"unsafe"

	"github.com/reiver/go-porterstemmer"
)

type UnrolledGroup struct {
	next int
	s    []uint32
}

var seperators = [6]byte{10, 32, 34, 44, 46, 59}
var stopWords map[string]bool
var groupLen int = 10
var groupLenFactor float32 = 1.2
var groupLenLimit int = 256
var dictionary = struct {
	m    map[string]*list.List
	keys []string
}{m: make(map[string]*list.List)}
var notStem = [3]string{
	"ion", "ions", "iowa",
}
var indexMemoryLimit uint32 = 512 * 1024 * 1024
var indexMemoryConsumed = struct {
	v uint32
}{v: 0}

func main() {
	var corpusPath string
	flag.StringVar(&corpusPath, "corpus", "data/corpus", "File path of the corpus.")
	flag.Parse()

	fmt.Println("Creating index.")
	dispatcher(corpusPath)
}

func CreateIndex(corpusPath string) {
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
	var size int = groupLen
	token = strings.ToLower(token)
	if _, ok := stopWords[token]; !ok {
		// Stem the token?
		notStemFlag := false
		for _, k := range notStem {
			if k == token {
				notStemFlag = true
			}
		}
		if !notStemFlag {
			token = porterstemmer.StemString(token)
		}

		postingsList, ok := dictionary.m[token]
		if ok {
			lastEl := postingsList.Back()
			lastGroup := lastEl.Value.(*UnrolledGroup)
			if lastGroup.next == len(lastGroup.s) {
				if len(lastGroup.s) != groupLenLimit {
					size = int(float32(len(lastGroup.s)) * groupLenFactor)
					if size > groupLenLimit {
						size = groupLenLimit
					}
				} else {
					size = groupLenLimit
				}
				newGroup := UnrolledGroup{
					next: 1,
					s:    make([]uint32, size),
				}
				newGroup.s[0] = pos
				postingsList.PushBack(&newGroup)
			} else {
				lastGroup.s[lastGroup.next] = pos
				lastGroup.next += 1
			}
		} else {
			l := list.New()
			newGroup := UnrolledGroup{next: 1, s: make([]uint32, groupLen)}
			newGroup.s[0] = pos
			l.PushBack(&newGroup)
			dictionary.m[token] = l
			dictionary.keys = append(dictionary.keys, token)
			memoryConsumed += uint32(unsafe.Sizeof(token))
		}
	}
	indexMemoryConsumed.v += memoryConsumed
}

func writeIndex(w io.Writer) {
	writer := bufio.NewWriter(w)
	var v *list.List
	out := make([]byte, 11) // Length of a uint32
	out[0] = ','
	buf := out[1:]
	for _, k := range dictionary.keys {
		v = dictionary.m[k]
		writer.WriteString("#" + k)
		var group *UnrolledGroup
		for el := v.Front(); el != nil; el = el.Next() {
			group = el.Value.(*UnrolledGroup)
			for i := 0; i < group.next; i++ {
				posting := group.s[i]
				binary.PutUvarint(buf, uint64(posting))
				writer.Write(out)
			}
		}
	}
	writer.Flush()
}

func splitTokens(data []byte, atEOF bool) (advance int, token []byte, err error) {
	var (
		found    = false
		started  = false
		complete = false
		offset   = 0
	)
	for i := 0; i < len(data); i++ {
		found = false
		for s := 0; s < len(seperators); s++ {
			if data[i] == seperators[s] {
				advance = i + 1
				token = data[offset:i]
				if !started {
					offset += 1
				} else {
					complete = true
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
	if !complete {
		return 0, nil, nil
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
