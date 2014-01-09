package main

import (
	"os"
)

type Tokenizer struct {
	f   *os.File
	buf []byte
}

func NewFileTokenizer(f *os.File) *Tokenizer {
	tokenizer := Tokenizer{f: f}
	return &tokenizer
}

func NewDirTokenizer(dir string) []*Tokenizer {
	dirF, err := os.Open(dir)
	defer dirF.Close()
	if err != nil {
		panic(err)
	}

	fileNames, err := dirF.Readdirnames(0)
	if err != nil {
		panic(err)
	}

	var tokenizers []*Tokenizer
	for _, fileName := range fileNames {
		fileName = dir + "/" + fileName
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}

		tokenizers = append(tokenizers, &Tokenizer{f: file})
	}

	return tokenizers
}

func (t *Tokenizer) GetToken() (token []byte, err error) {
	if len(t.buf) < 1 || t.buf == nil {
		t.buf = make([]byte, 4*1024)
		_, err = t.f.Read(t.buf)
		if err != nil {
			return
		}
	}

	found := false
	started := false
	start := 0
	end := 0
	var b byte
	for !found {
		for i := 0; i < len(t.buf); i++ {
			b = t.buf[i]
			if ((b >= 'A') && (b <= 'Z')) || ((b >= 'a') && (b <= 'z')) || b == '>' || b == '<' {
				if !started {
					started = true
					start = i
				}
			} else {
				if started {
					if i-start > 1 {
						found = true
						end = i
						break
					} else {
						started = false
						start = 0
					}
				}
			}
		}
		if !found {
			out := make([]byte, 4*1024)
			var n int
			n, err = t.f.Read(out)
			if err != nil || n == 0 {
				return
			}
			t.buf = append(t.buf, out...)
		}
	}
	if err != nil {
		token = nil
	} else {
		token = t.buf[start:end]
	}
	t.buf = t.buf[end:]
	return
}
