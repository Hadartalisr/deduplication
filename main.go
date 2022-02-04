package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	inputDirectoryPath = "generator/input"
	minChunkSize = 8
	maxChunkSize = 32
	readBufferSizeInBytes = 4*10
)

func main() {
	err := readFile()
	if err != nil {
		fmt.Printf("Error occured")
		print(err)
	}
}

func readFile() error{
	filePath :=  filepath.Join(inputDirectoryPath, "hello.txt")
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	buf := make([]byte, 0, readBufferSizeInBytes)
	fmt.Printf("Start Reading %s\n", filePath)
	for {
		// reading the file in buffer
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return err
		}
		if err != nil && err != io.EOF {
			return err
		}

		// performing process
		println(string(buf[:]))
	}
	return nil
}





