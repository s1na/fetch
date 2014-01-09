package main

import ()

func toLowerBytes(s []byte) []byte {
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			s[i] = c + 32
		}
	}
	return s
}

func isStopWord(s string) bool {
	_, ok := stopWords[s]
	return ok
}
