package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"container/list"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/reiver/go-porterstemmer"
)

type UnrolledGroup struct {
	next int
	s    []uint32
}
var dictionary = struct {
	m    map[string]*list.List
	keys []string
}{m: make(map[string]*list.List)}

const (
	groupLen = 10
	groupLenFactor float32 = 1.2
	groupLenLimit = 256
	indexMemoryLimit = 5 * 1024 * 1024
	indexPath = "index/"
	writerBufSize = 64 * 1024
	readerBufSize = 64 * 1024
	mergeOpenFiles = 5
)

var (
	indexMemoryConsumed = 0
	filesQueue = list.New()

	stopWords map[string]bool
	notStem = [3]string{
		"ion", "ions", "iowa",
	}
)

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

	collectStopWords()
	file, err := os.Open(corpusPath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	t := NewTokenizer(file)


	var (
		token string
		pos   uint32 = 1
		outPath string
	)
	for token, err = t.GetToken(); err==nil && len(token)!=0; token, err = t.GetToken() {
		token = token
		if indexMemoryConsumed >= indexMemoryLimit {
			sort.Strings(dictionary.keys)

			outPath = indexPath + "index."
			outPath += strconv.Itoa(filesQueue.Len() + 1) // Prevent file from having .0 suffix
			writeIndex(outPath)

			filesQueue.PushBack(outPath)
			clearMem()
		}
		addToken(token, pos)
		pos += 1
	}
	if err != nil && err != io.EOF {
		panic(err)
	}

	if indexMemoryConsumed > 0 {
		sort.Strings(dictionary.keys)

		outPath = indexPath + "index."
		outPath += strconv.Itoa(filesQueue.Len() + 1) // Prevent file from having .0 suffix
		writeIndex(outPath)

		filesQueue.PushBack(outPath)
		clearMem()
	}

	mergeAll()
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
	var memoryConsumed int = 0
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
			token = string(porterstemmer.StemWithoutLowerCasing([]rune(token)))
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
				memoryConsumed += int(unsafe.Sizeof(newGroup)) + size*4 + int(unsafe.Sizeof(lastEl))
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
			memoryConsumed += int(unsafe.Sizeof(l)) + int(unsafe.Sizeof(newGroup)) +
				int(unsafe.Sizeof(token)) + groupLen*4 +
				int(unsafe.Sizeof(token)) + 2*len(token)
		}
	}
	indexMemoryConsumed += memoryConsumed
}

func writeIndex(outPath string) {
	outFile, err := os.Create(outPath)
	defer outFile.Close()
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriterSize(io.Writer(outFile), writerBufSize)

	var v *list.List
	buf := make([]byte, 4) // Size of a uint32
	for _, k := range dictionary.keys {
		v = dictionary.m[k]
		writer.WriteString(k + ",")
		var group *UnrolledGroup
		for el := v.Front(); el != nil; el = el.Next() {
			group = el.Value.(*UnrolledGroup)
			for i := 0; i < group.next; i++ {
				posting := group.s[i]
				binary.PutUvarint(buf, uint64(posting))
				writer.Write(buf)
			}
		}
		writer.Write([]byte{0, 0, 0, 0})
	}
	writer.Flush()
}

func mergeAll() {
	if filesQueue.Len() == 1 {
		os.Rename(filesQueue.Remove(filesQueue.Front()).(string), indexPath + "index")
	}
	firstQueue := filesQueue
	secondQueue := list.New()
	activeQueue := 1
	var openFiles int
	merges := 1
	if firstQueue.Len()/mergeOpenFiles >= 1 {
		openFiles = mergeOpenFiles
	} else {
		openFiles = firstQueue.Len() % mergeOpenFiles
	}
	for openFiles > 0 {
		var curFiles []string
		var outPath string
		if (openFiles==mergeOpenFiles) || (activeQueue==1 && secondQueue.Len()!=0) ||
			(activeQueue==2 && firstQueue.Len()!=0) {
			if activeQueue == 1 {
				outPath = indexPath + "indexm." + strconv.Itoa(merges)
			} else {
				outPath = indexPath + "index." + strconv.Itoa(merges)
			}
		} else {
			outPath = indexPath + "index"
		}

		for i := 0; i < openFiles; i++ {
			var filePath string
			if activeQueue == 1 {
				firstEl := firstQueue.Front()
				filePath = firstQueue.Remove(firstEl).(string)
			} else {
				firstEl := secondQueue.Front()
				filePath = secondQueue.Remove(firstEl).(string)
			}
			curFiles = append(curFiles, filePath)
		}

		mergeFiles(outPath, curFiles[:])
		merges++
		if activeQueue == 1 {
			secondQueue.PushBack(outPath)
		} else {
			firstQueue.PushBack(outPath)
		}
		for _, f := range curFiles {
			os.Remove(f)
		}

		if activeQueue == 1 {
			if firstQueue.Len() == 0 {
				if secondQueue.Len() == 1 {
					os.Rename(secondQueue.Remove(secondQueue.Front()).(string), "index")
					openFiles = 0
					break
				} else {
					activeQueue = 2
					merges = 1
					if secondQueue.Len()/mergeOpenFiles >= 1 {
						openFiles = mergeOpenFiles
					} else {
						openFiles = secondQueue.Len() % mergeOpenFiles
					}
				}
			} else {
				if firstQueue.Len()/mergeOpenFiles >= 1 {
					openFiles = mergeOpenFiles
				} else {
					openFiles = firstQueue.Len() % mergeOpenFiles
				}
			}
		} else {
			if secondQueue.Len() == 0 {
				if firstQueue.Len() == 1 {
					os.Rename(firstQueue.Remove(firstQueue.Front()).(string), "index")
					openFiles = 0
					break
				} else {
					activeQueue = 1
					merges = 1
					if firstQueue.Len()/mergeOpenFiles >= 1 {
						openFiles = mergeOpenFiles
					} else {
						openFiles = firstQueue.Len() % mergeOpenFiles
					}
				}
			} else {
				if secondQueue.Len()/mergeOpenFiles >= 1 {
					openFiles = mergeOpenFiles
				} else {
					openFiles = secondQueue.Len() % mergeOpenFiles
				}
			}
		}
	}
}

func mergeFiles(outPath string, filePaths []string) {
	outFile, err := os.Create(outPath)
	defer outFile.Close()
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriterSize(io.Writer(outFile), writerBufSize)
	defer writer.Flush()

	readers := make([]*bufio.Reader, len(filePaths))
	orders := make([]int, len(filePaths))
	for in, filePath := range filePaths {
		file, err := os.Open(filePath)
		defer file.Close()
		if err != nil {
			panic(err)
		}
		reader := bufio.NewReaderSize(io.Reader(file), readerBufSize)
		orders[in], _ = strconv.Atoi(filePath[strings.LastIndex(filePath, ".") + 1:])
		readers[in] = reader
	}

	// Actually merge!
	type Term struct {
		token string
		orders []int
	}
	stop := false
	remainingFiles := len(readers)
	var curTerms []*Term
	var token string
	var curTerm *Term
	for in, reader := range readers {
		token, _ = reader.ReadString(byte(','))
		token = strings.Trim(token, ",")
		found := false
		for _, term := range(curTerms) {
			if term.token == token {
				term.orders = append(term.orders, orders[in])
				curTerm = term
				found = true
			}
		}
		if !found {
			var curOrders []int
			curOrders = append(curOrders, orders[in])
			curTerm = &Term{token: token, orders: curOrders}
			curTerms = append(curTerms, curTerm)
		}
		sort.Ints(curTerm.orders)
	}
	for !stop {
		// Min token
		if len(curTerms) == 0 {
			break
		}
		curTerm = curTerms[0]
		for _, term := range curTerms {
			if term.token < curTerm.token {
				curTerm = term
			}
		}

		writer.WriteString(curTerm.token + ",")
		var buf []byte = make([]byte, 4)
		var postingsEnd []byte = []byte{0, 0, 0, 0}
		var r byte
		var byteCounter int
		for _, order := range curTerm.orders {
			// Make function creates array with zero values
			// and sort brings them to the front, got to skip them
			if order == 0 {
				continue
			}
			for in, bOrder := range orders {
				if order!=bOrder || readers[in]==nil {
					continue
				}

				feof := false
				listEnded := false
				for !listEnded {
					for byteCounter = 0; byteCounter < 4; byteCounter++ {
						r, err = readers[in].ReadByte()
						if err != nil {
							if err == io.EOF {
								readers[in] = nil
								feof = true
								listEnded = true
								remainingFiles--
								break
							} else {
								panic(err)
							}
						}
						buf[byteCounter] = r
					}
					if bytes.Equal(buf, postingsEnd) {
						listEnded = true
					} else {
						if feof {
							panic("File ended, without a final 0000.")
						}
						writer.Write(buf)
					}
				}
				if feof {
					break
				}

				// Next token from the same reader
				token, _ = readers[in].ReadString(',')
				token = strings.Trim(token, ",")

				found := false
				for _, term := range curTerms {
					if term.token == token {
						term.orders = append(term.orders, orders[in])
						sort.Ints(term.orders)
						found = true
						break
					}
				}
				if !found {
					var curOrders []int
					curOrders = append(curOrders, orders[in])
					curTerms = append(curTerms, &Term{token: token, orders: curOrders})
				}
			}
		}
		writer.Write(postingsEnd)

		// Delete the current term
		for in, term := range curTerms {
			if term == curTerm {
				tmp := curTerms[0]
				curTerms[0] = curTerm
				curTerms[in] = tmp

			}
		}
		curTerms = curTerms[1:]

		if remainingFiles == 1 {
			stop = true
		}
	}
}

func clearMem() {
	dictionary.m = nil
	dictionary.m = make(map[string]*list.List)
	dictionary.keys = nil
	indexMemoryConsumed = 0
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
