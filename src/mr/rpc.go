package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)
import "strconv"

type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	ReduceCount int
}

func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Master.GetReduceCount", &args, &reply)

	return reply.ReduceCount, succ
}

type RequestTaskReply struct {
	State    TaskState
	TaskId   int
	TaskFile string
	NReduce  int
}

type RequestTaskArgs struct {
	WorkerId int
}

func requestTask(workerId int) (*RequestTaskReply, bool) {
	args := RequestTaskArgs{
		WorkerId: workerId,
	}
	reply := RequestTaskReply{}
	success := call("Master.RequestTask", &args, &reply)

	return &reply, success
}

type ReportTaskArgs struct {
	WorkerId int
	State    TaskState
	TaskId   int
}

type ReportTaskReply struct {
	CanExit bool
}

func reportTaskDone(state TaskState, taskId int, workerId int) (bool, bool) {
	args := ReportTaskArgs{
		WorkerId: workerId,
		State:    state,
		TaskId:   taskId,
	}
	reply := ReportTaskReply{}
	succ := call("Master.ReportTaskDone", &args, &reply)

	return reply.CanExit, succ
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
