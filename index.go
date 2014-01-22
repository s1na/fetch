package main

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/binary"
	//"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/s1na/go-porterstemmer"
)

type UnrolledGroup struct {
	next int
	s    []uint32
}

type Posting struct {
	doc uint32
	tf  uint32
	pos *list.List
}

type IDocInfo struct {
	length uint32
	pos    int64
}

var dictionary = struct {
	m    map[string]*list.List
	keys []string
}{m: make(map[string]*list.List)}

const (
	groupLen                 = 10
	groupLenFactor   float32 = 1.2
	groupLenLimit            = 256
	indexMemoryLimit         = 50 * 1024 * 1024
	indexPath                = "index/"
	writerBufSize            = 64 * 1024
	readerBufSize            = 512 * 1024
	mergeOpenFiles           = 5
)

var (
	indexMemoryConsumed = 0
	filesQueue          = list.New()

	docPrefix          = []byte{'<', 'R', 'E', 'U', 'T', 'E', 'R', 'S'}
	docId       uint32 = 0
	iDocInfos   []*IDocInfo
	uniqueTerms uint32 = 1
)

func createIndex(corpusPath string) {
	dispatcher(corpusPath)
}

func dispatcher(corpusPath string) {
	file, err := os.Open(corpusPath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	t := NewFileTokenizer(file)

	var (
		token   []byte
		outPath string
		pos     uint32 = 1
	)
	//counter := 0
	for token, err = t.GetToken(); err == nil && token != nil && len(token) != 0; token, err = t.GetToken() {
		/*if counter < 100 {
			fmt.Println(string(token))
			counter++
		}*/
		if token[0] == '<' {
			if bytes.HasPrefix(token, docPrefix) {
				docId += 1
				iDocInfos = append(iDocInfos, &IDocInfo{length: 0, pos: t.GetFilePos() - int64(len(token))})
				if indexMemoryConsumed >= indexMemoryLimit {
					sort.Strings(dictionary.keys)

					outPath = indexPath + "index."
					outPath += strconv.Itoa(filesQueue.Len() + 1) // Prevent file from having .0 suffix
					writeIndex(outPath)

					filesQueue.PushBack(outPath)
					clearMem()
				}
			}
		} else {
			addToken(token, pos)
			pos += 1
			iDocInfos[docId-1].length++
		}
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

	writeMetaData(indexPath + "metadata")
	//mergeAll()
}

func addToken(b []byte, pos uint32) {
	// Check to see if token is a stop word
	var memoryConsumed int = 0
	//var size int = groupLen
	var token string
	b = toLowerBytes(b)
	token = string(b)
	if !isStopWord(token) {
		// Stem the token?
		token = string(porterstemmer.StemWithoutLowerCasing(b))

		postingsList, ok := dictionary.m[token]
		if ok {
			lastPosting := postingsList.Back().Value.(*Posting)
			if lastPosting.doc == docId {
				lastPosting.pos.PushBack(pos)
				lastPosting.tf += 1
			} else {
				posList := list.New()
				posList.PushBack(pos)
				newPosting := Posting{doc: docId, tf: 1, pos: posList}
				postingsList.PushBack(&newPosting)

				memoryConsumed += int(unsafe.Sizeof(posList)) +
					int(unsafe.Sizeof(newPosting))
			}
			/*lastGroup := lastEl.Value.(*UnrolledGroup)
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
					s:    make([]*Posting, size),
				}
				memoryConsumed += int(unsafe.Sizeof(newGroup)) + size*4 + int(unsafe.Sizeof(lastEl))
				newGroup.s[0] = &Posting{pos: pos, doc: docId}
				postingsList.PushBack(&newGroup)
			} else {
				lastGroup.s[lastGroup.next] = &Posting{pos: pos, doc: docId}
				lastGroup.next += 1
			}*/
		} else {
			l := list.New()
			posList := list.New()
			//newGroup := UnrolledGroup{next: 1, s: make([]uint32, groupLen)}
			//newGroup.s[0] = pos
			posList.PushBack(pos)
			newPosting := Posting{doc: docId, tf: 1, pos: posList}
			l.PushBack(&newPosting)
			dictionary.m[token] = l
			dictionary.keys = append(dictionary.keys, token)
			uniqueTerms++
			memoryConsumed += int(unsafe.Sizeof(l)) + int(unsafe.Sizeof(pos)) +
				int(unsafe.Sizeof(posList)) + int(unsafe.Sizeof(newPosting)) +
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
	dBuf := make([]byte, 4)
	tfBuf := make([]byte, 4)
	pBuf := make([]byte, 4)
	for _, k := range dictionary.keys {
		v = dictionary.m[k]
		writer.WriteString(k + ",")
		var posting *Posting
		for el := v.Front(); el != nil; el = el.Next() {
			posting = el.Value.(*Posting)
			binary.PutUvarint(dBuf, uint64(posting.doc))
			writer.Write(dBuf)
			dBuf = []byte{0, 0, 0, 0}
			binary.PutUvarint(tfBuf, uint64(posting.tf))
			writer.Write(tfBuf)
			tfBuf = []byte{0, 0, 0, 0}
			for posEl := posting.pos.Front(); posEl != nil; posEl = posEl.Next() {
				pos := posEl.Value.(uint32)
				binary.PutUvarint(pBuf, uint64(pos))
				writer.Write(pBuf)
				pBuf = []byte{0, 0, 0, 0}
			}
		}
		writer.Write([]byte{0, 0, 0, 0})
	}
	writer.Flush()
}

func writeMetaData(outPath string) {
	outFile, err := os.Create(outPath)
	defer outFile.Close()
	if err != nil {
		panic(err)
	}
	var buf []byte = make([]byte, 4)
	var posBuf []byte = make([]byte, 8)
	binary.PutUvarint(buf, uint64(uniqueTerms))
	outFile.Write(buf)
	buf = []byte{0, 0, 0, 0}
	binary.PutUvarint(buf, uint64(docId))
	outFile.Write(buf)
	buf = []byte{0, 0, 0, 0}

	var docIdInt int = int(docId)
	for i := 0; i < docIdInt; i++ {
		binary.PutUvarint(buf, uint64(iDocInfos[i].length))
		outFile.Write(buf)
		buf = []byte{0, 0, 0, 0}
		binary.PutUvarint(posBuf, uint64(iDocInfos[i].pos))
		outFile.Write(posBuf)
		posBuf = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	}
}

func mergeAll() {
	if filesQueue.Len() == 1 {
		os.Rename(filesQueue.Remove(filesQueue.Front()).(string), indexPath+"index")
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
		if (openFiles == mergeOpenFiles) || (activeQueue == 1 && secondQueue.Len() != 0) ||
			(activeQueue == 2 && firstQueue.Len() != 0) {
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
		orders[in], _ = strconv.Atoi(filePath[strings.LastIndex(filePath, ".")+1:])
		readers[in] = reader
	}

	// Actually merge!
	type Term struct {
		token  string
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
		for _, term := range curTerms {
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
		var buf []byte = make([]byte, 8)
		var posBuf []byte = make([]byte, 4)
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
				if order != bOrder || readers[in] == nil {
					continue
				}

				feof := false
				listEnded := false
				for !listEnded {
					for byteCounter = 0; byteCounter < 8; byteCounter++ {
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
					if feof {
						panic("File ended, without a final 0000.")
					}
					writer.Write(buf)

					tf, n := binary.Uvarint(buf[4:8])
					if n <= 0 {
						panic("Failed while converting tf in merge.")
					}
					for i := 0; i < int(tf); i++ {
						for j := 0; j < 4; j++ {
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
							posBuf[byteCounter] = r
						}
						if bytes.Equal(posBuf, postingsEnd) {
							listEnded = true
							break
						} else {
							if feof {
								panic("File ended, without a final 0000.")
							}
							writer.Write(posBuf)
						}
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

func readIndex(indexPath string) {
	file, err := os.Open("index/index.1")
	defer file.Close()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(io.Reader(file))
	var buf []byte = make([]byte, 8)
	var posBuf []byte = make([]byte, 4)
	var r byte
	var offset int64 = 0
	var byteCounter int
	var postingsEnd []byte = []byte{0, 0, 0, 0}
	var term string

	term, _ = reader.ReadString(',')
	offset += int64(len(term))
	cur_term_list := list.New()
	dict = append(dict, &Term{term: strings.Trim(term, ","), docs: cur_term_list, pos: offset})
	for {
		end := false
		for !end {
			for byteCounter = 0; byteCounter < 8; byteCounter++ {
				r, err = reader.ReadByte()
				if err != nil {
					if err == io.EOF {
						end = true
					}
					break
				}
				offset++
				buf[byteCounter] = r
				if byteCounter == 3 {
					if bytes.Equal(buf[:4], postingsEnd) {
						end = true
						break
					}
				}
			}
			if end {
				break
			}
			doc, _ := binary.Uvarint(buf[:4])
			tf, _ := binary.Uvarint(buf[4:])
			cur_term_list.PushBack(&Document{docId: int(doc), tf: int(tf)})
			for i := 0; i < int(tf); i++ {
				for j := 0; j < 4; j++ {
					r, err = reader.ReadByte()
					if err != nil {
						if err == io.EOF {
							end = true
						}
						break
					}
					offset++
					posBuf[j] = r
				}
			}
		}
		term, err = reader.ReadString(',')
		if err != nil {
			break
		}
		offset += int64(len(term))
		cur_term_list = list.New()
		dict = append(dict, &Term{term: strings.Trim(term, ","), docs: cur_term_list, pos: offset})
	}
	if err != nil {
		if err != io.EOF {
			panic(err)
		}
	}
}

func readMetaData(inPath string) {
	inFile, err := os.Open(inPath)
	defer inFile.Close()
	if err != nil {
		panic(err)
	}
	var buf []byte = make([]byte, 8)
	var posBuf []byte = make([]byte, 8)
	inFile.Read(buf)
	tmp, _ := binary.Uvarint(buf[:4])
	totalTerms = int(tmp)
	tmp, _ = binary.Uvarint(buf[4:])
	totalDocs = int(tmp)

	buf = make([]byte, 4)
	docInfos = make([]*DocInfo, totalDocs)
	for i := 0; i < totalDocs; i++ {
		inFile.Read(buf)
		tmp, _ = binary.Uvarint(buf)
		inFile.Read(posBuf)
		pos, _ := binary.Uvarint(posBuf)
		docInfos[i] = &DocInfo{length: int(tmp), pos: int64(pos)}

		docLenAvg += float64(tmp)
	}
	docLenAvg = docLenAvg / float64(totalDocs)
}
