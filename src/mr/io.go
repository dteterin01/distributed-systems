package mr

import (
	"encoding/json"
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

func readKVResultFromFile(filenames []string) (map[string][]string, error) {
	kvMap := make(map[string][]string)

	// read input file
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if checkError(err, fmt.Sprintf("Failed during open file %s", filename)) {
			return nil, err
		}

		defer file.Close()

		decoder := json.NewDecoder(file)
		var kv KeyValue

		for decoder.More() {
			err := decoder.Decode(&kv)
			checkError(err, "Failed to decode key-value pair.")
			if err != nil {
				return nil, err
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	return kvMap, nil
}
