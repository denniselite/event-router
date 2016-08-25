package log

import "log"

var strings []string

func Print(value string) {
	strings = append(strings, value)
}

func Write() {
	log.Printf("%V", strings)
}