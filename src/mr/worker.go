package mr

import (
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

	info := WorkerInfo{
		mapF:     mapF,
		reduceF:  reduceF,
		workerId: os.Getpid(),
	}

	info.scheduleWorkStealing()
}

func (w *WorkerInfo) scheduleWorkStealing() {

	for true {
		reply, success := requestTask(w.workerId)

		if success == false {
			log.Fatal("Failed to contact master worker exiting")
			return
		}

		if reply.state == ExitState {
			log.Fatal("All tasks done worker exiting.")
			return
		}

		exit, succ := false, true
		switch reply.state {
		case NoTask:
			// All map/reduce task are schedule
			break
		case MapState:
			mapState(w.workerId, reply.taskId, w.reduceCount, reply.taskFile, w.mapF)
			exit, succ = reportTaskDone(MapState, reply.taskId, w.workerId)
		case ReduceState:
			reduceState(w.workerId, reply.taskId, w.reduceCount, "", w.reduceF)
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
