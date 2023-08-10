package mr

import (
	"fmt"
	"os"
	"time"
)
import "log"

const TaskInterval = 200

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	mapF        func(string, string) []KeyValue
	reduceF     func(string, []string) string
	workerId    int
	reduceCount int
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	workerId := os.Getpid()

	info := WorkerInfo{
		mapF:     mapF,
		reduceF:  reduceF,
		workerId: workerId,
	}

	info.schedule()
}

func (w *WorkerInfo) schedule() {

	n, succ := getReduceCount()
	if succ == false {
		fmt.Println("Failed to get reduce task count, worker exiting.")
		return
	}
	w.reduceCount = n

	for {
		reply, success := requestTask(w.workerId)

		if success == false {
			log.Fatal("Failed to contact master worker exiting")
			return
		}

		if reply.State == ExitState {
			log.Fatal("All tasks done worker exiting.")
			return
		}

		exit, succ := false, true
		switch reply.State {
		case NoTask:
			// All map/reduce task are schedule
			break
		case MapState:
			mapState(w.workerId, reply.TaskId, w.reduceCount, reply.TaskFile, w.mapF)
			exit, succ = reportTaskDone(MapState, reply.TaskId, w.workerId)
		case ReduceState:
			reduceState(w.workerId, reply.TaskId, w.reduceF)
			exit, succ = reportTaskDone(ReduceState, reply.TaskId, w.workerId)
		default:
			break
		}

		if exit || !succ {
			log.Println("Master exited or all tasks done, worker exiting.")
			return
		}

		time.Sleep(time.Millisecond * TaskInterval)
	}
}
