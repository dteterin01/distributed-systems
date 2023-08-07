package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
)

func mapState(
	workerId int,
	mapTask int,
	nReduce int, // count of tmp files

	fileName string,
	mapF func(filename string, contents string) []KeyValue,
) {
	content, err := readFile(fileName)
	if checkError(err, fmt.Sprintf("Failed to map function for file: %s", fileName)) {
		return
	}

	result := mapF(fileName, string(content))
	err = preparerMapResultForReduceAndSaveOnFS(workerId, mapTask, nReduce, result)
	if checkError(err, fmt.Sprintf("Failed while create reduce files for file: %s", fileName)) {
		return
	}
}

func preparerMapResultForReduceAndSaveOnFS(
	workerId int,
	mapTask int,
	nReduce int,
	content []KeyValue,
) error {
	encoders := make([]*json.Encoder, nReduce)
	for i := range encoders {
		filename := reduceName(workerId, mapTask, i)
		file, err := os.Create(filename)

		if err != nil {
			return err
		}

		file.Close()
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range content {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		if checkError(err, "Failed to encode key-value pair.") {
			return err
		}
	}
	return nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
