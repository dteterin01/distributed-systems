package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func reduceState(
	workerId int,
	reduceTask int,
	reduceF func(key string, values []string) string,
) {
	files, _ := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceTask))
	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filePath := range files {
		file, _ := os.Open(filePath)

		dec := json.NewDecoder(file)
		for dec.More() {
			dec.Decode(&kv)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	writeReduceOutput(reduceF, kvMap, reduceTask, workerId)
}

func writeReduceOutput(
	reducef func(string, []string) string,
	kvMap map[string][]string,
	reduceId int,
	workerId int) {

	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, workerId)
	file, _ := os.Create(filePath)

	// Call reduce and write to temp file
	for _, k := range keys {
		fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	os.Rename(filePath, newPath)
}
