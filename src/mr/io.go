package mr

import (
	"fmt"
	"os"
)

func readFile(filename string) ([]byte, error) {
	// read input file
	file, err := os.Open(filename)
	if checkError(err, fmt.Sprintf("Failed during open file %s", filename)) {
		return make([]byte, 0), err
	}

	fileInfo, err := file.Stat()
	if checkError(err, fmt.Sprintf("Failed during get file stat %s", filename)) {
		return make([]byte, 0), err
	}

	contents := make([]byte, fileInfo.Size())

	_, err = file.Read(contents)
	if checkError(err, fmt.Sprintf("Failed during read file %s", filename)) {
		return make([]byte, 0), err
	}

	err = file.Close()
	if checkError(err, fmt.Sprintf("Failed during close file %s", filename)) {
		return contents, err
	}

	return contents, nil
}
