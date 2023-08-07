package mr

import (
	"encoding/json"
	"os"
)

func reduceState(
	workerId int,
	reduceTask int,
	nMap int,

	outputFilename string,
	reduceF func(key string, values []string) string,
) {
	generatedFilenames := make([]string, nMap)
	for i, _ := range generatedFilenames {
		generatedFilenames[i] = reduceName(workerId, i, reduceTask)
	}

	kvResult, err := readKVResultFromFile(generatedFilenames)
	if err != nil {
		return
	}

	file, err := os.Create(outputFilename)
	checkError(err, "Failed to create output file.")
	defer file.Close()
	encoder := json.NewEncoder(file)

	for key, values := range kvResult {
		reducedValue := reduceF(key, values)
		kv := KeyValue{key, reducedValue}
		err := encoder.Encode(&kv)
		checkError(err, "Failed to encode key-value pair.")
	}
}
