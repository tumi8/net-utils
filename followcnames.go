package main

import (
	"bufio"
	"compress/gzip"
	// "fmt"
	"bytes"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"unicode"
	"flag"
)

var compressed = flag.Bool("compressed", true, "is the input file compressed")
var space= flag.Bool("space", false, "false = tab, true = space as seperator")

// Global variables for CNAME and IN lookup
var cnames map[string][]string
var ins map[string][]string
var logger *log.Logger


var A = []byte("A")
var AAAA = []byte("AAAA")
var SRV = []byte("SRV")
var DNAME = []byte("DNAME")
var CNAME = []byte("CNAME")

const limit = 20

type record struct {
	domain []byte
	value []byte
}

func init() {
	// Set the number of Go processes to 6
	runtime.GOMAXPROCS(6)
}

func contains(s []string, e string) bool {
	if len(s) > 0 {
		for _, a := range s {
			if a == e {
				return true
			}
		}
	}
	return false
}

func ToLower(value []byte) []byte {
	if len(value) != 0 {
		for i, b := range value {
			value[i] = byte(unicode.ToLower(rune(b)))
		}
	}
	return value
}

func ToCanonical(value []byte) []byte {
	valLen := len(value)
	if valLen == 0 {
		return value
	} else if value[valLen - 1] == '\n' {
		if value[valLen - 2] == '.' {
			return value[0:valLen - 1]
		} else {
			value[valLen - 1] = '.'
			return value
		}
	} else {
		if value[valLen - 1] == '.' {
			return value
		} else {
			value = append(value, '.')
			return value
		}
	}
}

func GetIdx(line []byte) []int {
	var id []int
	for i, b := range line {
		if *space {
			if b == ' ' {
				id = append(id, i)
			}
		} else {
			if b == '\t' {
				id = append(id, i)
			}
		}

	}
	return id
}

func cnameLookup(origDomain, currDomain string, level int) {

	if level > limit {
		log.Printf("followcnames: depth exceeded for domain %s from origdomain %s \n", currDomain, origDomain)
		return
	}

	if ipMap, ok := ins[currDomain]; ok {
		for _, ip := range ipMap {
			if ip == "\\# 0" {
				ip = "\\#"
			}
			logger.Printf("%s,%s\n",ip, origDomain)
		}
	}

	if domainMap, ok := cnames[currDomain]; ok {
		for _, domain := range domainMap {
			if domain == origDomain {
				log.Printf("followcnames: circle for domain %s from origdomain %s \n", currDomain, origDomain)
			} else {
				cnameLookup(origDomain, domain, level+1)
			}
		}
	}
}

func processRecords(recordChan <-chan []byte, wg *sync.WaitGroup) {

	wg.Add(1)

	for record := range recordChan {
		// Canonicalize input (finaldot, tolower(),…)
		record = ToCanonical(record)
		record = ToLower(record)
		canon := string(record)
		cnameLookup(canon, canon, 0)
	}

	wg.Done()
}

func readInput(recordChan chan<- []byte, inFile, domainFile string, wg *sync.WaitGroup) {

	// Close channel at the end of input sending
	defer close(recordChan)

	// Read input file into two dicts

	var rrType []byte
	var idx []int

	fh, _ := os.Open(inFile)
	scanner2 := bufio.NewReader(fh)
	lines := 0
	for true {
		sc, _, err := scanner2.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		idx = GetIdx(sc)
		if len(idx) != 4 {
			log.Fatal("incorrect number of fields:" + string(sc))
			break
		}

		rrType = sc[idx[2]+1:idx[3]]

		if bytes.Equal(rrType, CNAME) {
			lines += 1
		}
	}

	log.Printf("Line: %d", lines)

	cnames = make(map[string][]string,lines)
	req := make (map[string]bool,lines)

	fh, _ = os.Open(inFile)
	scanner2 = bufio.NewReader(fh)

	for true {
		sc, _, err := scanner2.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}

		idx = GetIdx(sc)
		if len(idx) != 4 {
			log.Fatal("incorrect number of fields:" + string(sc))
			break
		}

		rrType = sc[idx[2]+1:idx[3]]

		if bytes.Equal(rrType, CNAME) {
			domain := string(sc[0:idx[0]])
			value := string(sc[idx[3]+1:])
			_, ok := cnames[domain]

			if !ok {
				cnames[domain] = make([]string,0,1)
				cnames[domain] = append(cnames[domain], value)
			} else {

				if !contains(cnames[domain], value) {
					cnames[domain] = append(cnames[domain], value)
				}
			}
			req[value] = true

		} else if bytes.Equal(rrType, A) || bytes.Equal(rrType, AAAA) || bytes.Equal(rrType, SRV) || bytes.Equal(rrType, DNAME){
			// ignore the very few DNAMEs we have
			continue
		} else {
			panic("Should not happen: incorrect rrType = " + string(rrType))
		}
		idx = []int{}
	}

	ins = make(map[string][]string,len(req))

	fh.Close()
	log.Print("Finished Phase 1")
	fh, _ = os.Open(inFile)
	scanner2 = bufio.NewReader(fh)
	for true {

		sc, _, err := scanner2.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}

			break
		}
		idx = GetIdx(sc)
		if len(idx) != 4 {
			log.Fatal("incorrect number of fields:" + string(sc))
			break
		}

		rrType = sc[idx[2]+1:idx[3]]
		if bytes.Equal(rrType, A) || bytes.Equal(rrType, AAAA) || bytes.Equal(rrType, SRV) {
			domain := string(sc[0:idx[0]])
			value := string(sc[idx[3]+1:])
			if _, ok := req[domain]; ok {
				if _, ok := ins[domain]; ok {
					if !contains(ins[domain], value) {
						ins[domain] = append(ins[domain], value)
					}
				} else {
					ins[domain] = []string{value}
				}
			} else {
				if value == "\\# 0" {
					value = "\\#"
				}
				logger.Printf("%s,%s\n",value, domain)
			}
		} else if bytes.Equal(rrType, DNAME) || bytes.Equal(rrType, CNAME) {
			// ignore the very few DNAMEs we have
			continue
		} else {
			panic("Should not happen: incorrect rrType = " + string(rrType))
		}
		sc = []byte{}
		idx = []int{}
	}

	fh.Close()

	log.Print("Input Read")

	req = nil

	fh, err := os.Open(domainFile)
	defer fh.Close()
	if err != nil {
		log.Fatal(err)
	}
	var scanner *bufio.Scanner
	if *compressed {
		zr, err := gzip.NewReader(fh)
		defer zr.Close()
		if err != nil {
			log.Fatal(err)
		}
		scanner = bufio.NewScanner(zr)
	}	else {
		scanner = bufio.NewScanner(fh)
	}


	for scanner.Scan() {
		tmp := scanner.Bytes()
		record := make([]byte, len(tmp))
		copy(record,tmp)
		recordChan <- record
	}
	if err := scanner.Err(); err != nil {
		log.Println("Reading standard input:", err)
	}
	wg.Done()
}

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		os.Exit(1)
	}
	domainFile := flag.Arg(0)
	inFile := flag.Arg(1)

	logger = log.New(os.Stdout, "", 0)

	numRoutines := 40
	recordChan := make(chan []byte)

	// wg makes sure that all processing goroutines have terminated before exiting
	var wg sync.WaitGroup
	// This 1 is for the main goroutine and makes sure that the output is not immediately closed
	wg.Add(1)

	// Start goroutines for record processing
	for i := 0; i < numRoutines; i++ {
		go processRecords(recordChan, &wg)
	}

	// Start goroutine for input CSV reading
	go readInput(recordChan, inFile, domainFile, &wg)

	wg.Wait()
}
