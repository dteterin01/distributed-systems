package mr

import (
	"bufio"
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
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapTask)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, workerId)
		file, _ := os.Create(filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write map outputs to temp files
	for _, kv := range content {
		idx := ihash(kv.Key) % nReduce
		encoders[idx].Encode(&kv)
	}

	// flush file buffer to disk
	for _, buf := range buffers {
		buf.Flush()
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		os.Rename(file.Name(), newPath)
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
