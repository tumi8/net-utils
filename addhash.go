package main

import (
	"bufio"
	"os"
	"fmt"
	"hash/crc32"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	b := bufio.NewWriter(os.Stdout)
	for scanner.Scan() {
			t := scanner.Text()
			//tb := []byte(t)
	    //fmt.Println(crc32.ChecksumIEEE(tb),","+t)
			//fmt.Println(crc32.ChecksumIEEE([]byte(t)),","+t)
			//str := (crc32.ChecksumIEEE([]byte(t) + "," + t)
			fmt.Fprintln(b, crc32.ChecksumIEEE([]byte(t)), ","+t)
	}

	if scanner.Err() != nil {
	    // handle error.
	}
	b.Flush()
}