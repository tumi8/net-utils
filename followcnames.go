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

	"github.com/jessevdk/go-flags"
)

type out struct {
	value string
	domain string
}

var opts struct {
	Args struct {
		DomainFile flags.Filename `description:"Input: gzipped domain file containinig original domains" value-name:"DOMAINFILE"`
		InFile     flags.Filename `description:"Input: massdns result file" value-name:"INFILE"`
		//OutFile    flags.Filename `description:"Output file containing IP,domain" value-name:"OUTFILE"`
	} `positional-args:"yes" required:"yes"`
}

// Global variables for CNAME and IN lookup
var cnames map[string][]string
var ins map[string][]string

var A = []byte("A")
var AAAA = []byte("AAAA")
var SRV = []byte("SRV")
var DNAME = []byte("DNAME")
var CNAME = []byte("CNAME")

const limit = 20

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
		if b == '\t'{
			id = append(id,i)
		}
	}
	return id
}

func cnameLookup(origDomain, currDomain string, level int, outputChan chan<- out) {

	if level > limit {
		log.Printf("followcnames: depth exceeded for domain %s from origdomain %s \n", currDomain, origDomain)
		return
	}

	if ipMap, ok := ins[currDomain]; ok {
		for _, ip := range ipMap {
			outputChan <- out{ip, origDomain}
		}
	}

	if domainMap, ok := cnames[currDomain]; ok {
		for _, domain := range domainMap {
			if domain == origDomain {
				log.Printf("followcnames: circle for domain %s from origdomain %s \n", currDomain, origDomain)
			} else {
				cnameLookup(origDomain, domain, level+1, outputChan)
			}
		}
	}
}

func processRecords(recordChan <-chan []byte, outputChan chan<- out, wg *sync.WaitGroup) {

	wg.Add(1)

	for record := range recordChan {
		// Canonicalize input (finaldot, tolower(),…)
		record = ToCanonical(record)
		record = ToLower(record)
		canon := string(record)
		cnameLookup(canon, canon, 0, outputChan)
	}

	wg.Done()
}

func outputResult(outputChan <-chan out, wg *sync.WaitGroup) {
	logger := log.New(os.Stdout, "", 0)
	for res := range outputChan {
		// Backward compatibility
		if res.value == "\\# 0" {
			res.value = "\\#"
		}
		// most recent: decision to *not* drop any outputs, as this is systematically done at later steps
		// TODO: once stable, drop all the short and crappy outputs
		//if (len(res[0]) < 3) || (len(res[1]) <3) && (res[0] != "::" ) && (res[0] != "\\#") {
		//	log.Printf("Short output: " + res[0] + "," + res[1] + "\n")
		//}
		// fmt.Print(res[0] + "," + res[1] + "\n")
		// print should do, and this is only one routine, but lets try this anyway:
		// https://stackoverflow.com/questions/14694088/is-it-safe-for-more-than-one-goroutine-to-print-to-stdout/43327441#43327441
		logger.Printf("%s,%s\n",res.value, res.domain)
	}
	wg.Done()
}

func readInput(recordChan chan<- []byte, outputChan chan<- out, filename string) {

	// Close channel at the end of input sending
	defer close(recordChan)

	// Read input file into two dicts
	cnames = make(map[string][]string)
	ins = make(map[string][]string)

	fh, _ := os.Open(filename)
	scanner2 := bufio.NewReader(fh)
	var err error
	var sc []byte
	var rrType []byte
	var idx []int
	for true {
		sc, _, err = scanner2.ReadLine()
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
				cnames[domain] = []string{value}
			} else {

				if !contains(cnames[domain], value) {
					cnames[domain] = append(cnames[domain], value)
				}
			}
			ins[value] = []string{}
		} else if bytes.Equal(rrType, A) || bytes.Equal(rrType, AAAA) || bytes.Equal(rrType, SRV) || bytes.Equal(rrType, DNAME){
			// ignore the very few DNAMEs we have
			continue
		} else {
			panic("Should not happen: incorrect rrType = " + string(rrType))
		}
		sc = []byte{}
		idx = []int{}
	}
	fh.Close()
	log.Print("Finished Phase 1")
	fh, _ = os.Open(filename)
	scanner2 = bufio.NewReader(fh)
	for true {

		sc, _, err = scanner2.ReadLine()
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
			if _, ok := ins[domain]; ok {
				if !contains(ins[domain], value) {
					ins[domain] = append(ins[domain], value)
				}
			} else {
				outputChan <- out{value, domain}
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

	i := 0
	for domain, ips := range ins {
		if len(ips) == 0 {
			i+=1
			delete(ins, domain)
		}
	}

	log.Printf("Input Clean, %d", i)

	fh, err = os.Open(string(opts.Args.DomainFile))
	defer fh.Close()
	if err != nil {
		log.Fatal(err)
	}

	zr, err := gzip.NewReader(fh)
	defer zr.Close()
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(zr)
	for scanner.Scan() {
		recordChan <- []byte(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Println("Reading standard input:", err)
	}

	// scanner := bufio.NewReader(zr)
	//for true {
	//	sc, _, err := scanner.ReadLine()
	//	if err != nil {
	//		// if err != io.EOF {
	//		// 	log.Println(err)
	//		// }
	//
	//		break
	//	}
	//	recordChan <- sc

	//}
}

func main() {

	// Parse command line arguments
	parser := flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if err.(*flags.Error).Type == flags.ErrHelp {
			return
		} else if err.(*flags.Error).Type != flags.ErrRequired {
			log.Fatal(err)
		} else {
			os.Exit(1)
		}
	}

	numRoutines := 40
	recordChan := make(chan []byte)
	outputChan := make(chan out)

	// wg makes sure that all processing goroutines have terminated before exiting
	var wg sync.WaitGroup
	// This 1 is for the main goroutine and makes sure that the output is not immediately closed
	wg.Add(1)

	// wg makes sure that all processing goroutines have terminated before exiting
	var wgEnd sync.WaitGroup
	// This 1 is for the main goroutine and makes sure that the output is not immediately closed
	wgEnd.Add(1)

	go func() {
		// Close output channel when all processing goroutines finish
		defer close(outputChan)
		wg.Wait()
	}()

	// Start goroutines for record processing
	for i := 0; i < numRoutines; i++ {
		go processRecords(recordChan, outputChan, &wg)
	}

	go outputResult(outputChan, &wgEnd)

	// Start goroutine for input CSV reading
	go readInput(recordChan, outputChan, string(opts.Args.InFile))

	wg.Done()

	wgEnd.Wait()
}
