package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/jessevdk/go-flags"
)

var opts struct {
	Args struct {
		DomainFile flags.Filename `description:"Gzipped domain file containinig original domains" value-name:"DOMAINFILE"`
		InFile     flags.Filename `description:"Input file containinig massdns results" value-name:"INFILE"`
		//OutFile    flags.Filename `description:"Output file containing IP,domain" value-name:"OUTFILE"`
	} `positional-args:"yes" required:"yes"`
}

// Global variables for CNAME and IN lookup
var cnames map[string](map[string]bool)
var ins map[string](map[string]bool)

const limit = 20

func init() {
	// Set the number of Go processes to 6
	runtime.GOMAXPROCS(6)
}

func cnameLookup(origDomain, currDomain string, level int, outputChan chan<- []string) {

	if level > limit {
		log.Printf("followcnames: depth exceeded for domain %s from origdomain %s \n", currDomain, origDomain)
		return
	}

	if ipMap, ok := ins[currDomain]; ok {
		for ip, _ := range ipMap {
			outputChan <- []string{ip, origDomain}
		}
	}

	if domainMap, ok := cnames[currDomain]; ok {
		for domain, _ := range domainMap {
			cnameLookup(origDomain, domain, level+1, outputChan)
		}
	}
}

func processRecords(recordChan <-chan string, outputChan chan<- []string, wg *sync.WaitGroup) {

	wg.Add(1)

	for record := range recordChan {
		// Canonicalize input (finaldot, tolower(),…)
		canon := record
		if !strings.HasSuffix(canon, ".") {
			canon += "."
		}
		canon = strings.ToLower(canon)
		cnameLookup(canon, canon, 0, outputChan)
	}

	wg.Done()
}

func outputResult(outputChan <-chan []string) {
	// stdout := false
	// if opts.Args.OutFile == "-" {
	//		stdout = true
	//		w := false
	//} else {
	//	fh, err := os.OpenFile(string(opts.Args.OutFile), os.O_RDWR|os.O_CREATE, 0755)
	//
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	defer fh.Close()
	// 	w := csv.NewWriter(fh)
	// 	defer w.Flush()
	// }
	// i := 0

	for res := range outputChan {
		// Backward compatibility
		if res[0] == "\\# 0" {
			res[0] = "\\#"
		}
		// if !stdout{
		// 	w.Write(res)
		// 	if i == 10000 {
		// 		i = 0
		// 		w.Flush()
		// 	}
		// 	i += 1
		// } else {
		fmt.Println(res[0] + "," + res[1] + "\n")
	}
}

func readInput(recordChan chan<- string, outputChan chan<- []string, filename string) {

	// Close channel at the end of input sending
	defer close(recordChan)

	// Read input file into two dicts
	cnames = make(map[string](map[string]bool))
	ins = make(map[string](map[string]bool))

	fh, _ := os.Open(filename)
	r := csv.NewReader(bufio.NewReader(fh))
	r.Comma = '\t'
	r.LazyQuotes = true

	for {
		record, err := r.Read()

		// Stop at EOF
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println(err)
			continue
		}
		// make sure we read 5 records
		if len(record) < 5 {
			log.Fatal("incorrect number of fields")
			break
		}

		domain := record[0]
		rrType := record[3]
		value := record[4]
		var correctDict map[string](map[string]bool)

		if rrType == "CNAME" {
			correctDict = cnames
		} else if rrType == "A" || rrType == "AAAA" {
			correctDict = ins
		} else {
			panic("Should not happen: incorrect rrType = " + rrType)
		}

		domainSet, ok := correctDict[domain]
		if !ok {
			domainSet = make(map[string]bool)
		}
		domainSet[value] = true

		correctDict[domain] = domainSet
	}
	fh.Close()

	fh, err := os.Open(string(opts.Args.DomainFile))
	if err != nil {
		log.Fatal(err)
	}
	zr, err := gzip.NewReader(fh)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(zr)
	for scanner.Scan() {
		recordChan <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		log.Println("Reading standard input:", err)
	}
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

	numRoutines := 10000
	recordChan := make(chan string)
	outputChan := make(chan []string)

	// wg makes sure that all processing goroutines have terminated before exiting
	var wg sync.WaitGroup
	// This 1 is for the main goroutine and makes sure that the output is not immediately closed
	wg.Add(1)

	go func() {
		// Close output channel when all processing goroutines finish
		defer close(outputChan)
		wg.Wait()
	}()

	// Start goroutines for record processing
	for i := 0; i < numRoutines; i++ {
		go processRecords(recordChan, outputChan, &wg)
	}

	// Start goroutine for input CSV reading
	go readInput(recordChan, outputChan, string(opts.Args.InFile))

	wg.Done()

	// Start output writing
	outputResult(outputChan)
}
