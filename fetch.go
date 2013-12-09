package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	//"errors"
	"sync"

	"github.com/reiver/go-porterstemmer"
)

var seperators []byte = []byte{10, 32, 44, 46}

func main() {
	var corpusPath string
	flag.StringVar(&corpusPath, "corpus", "data", "File path of the corpus.")
	flag.Parse()
	dispatcher(corpusPath)
}

func dispatcher(corpusPath string) {
	file, err := os.Open(corpusPath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(io.Reader(file))

	scanner.Split(splitTokens)

	var token string
	var wg sync.WaitGroup
	for scanner.Scan() {
		token = scanner.Text()
		if len(token) > 0 {
			wg.Add(1)
			go addToken(token, &wg)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	wg.Wait()
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

func addToken(token string, wg *sync.WaitGroup) {
	defer wg.Done()
	token = porterstemmer.StemString(token)
	fmt.Println(token)
}
