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

type RequestTaskReply struct {
	state    TaskState
	taskId   int
	taskFile string
	nReduce  int
}

type RequestTask struct {
	workerId int
}

func requestTask(workerId int) (*RequestTaskReply, bool) {
	requestArgs := RequestTask{
		workerId: workerId,
	}
	reply := RequestTaskReply{}
	success := call("Master.RequestTask", &requestArgs, &reply)

	return &reply, success
}

type ReportTaskArgs struct {
	workerId int
	state    TaskState
	taskId   int
}

type ReportTaskReply struct {
	CanExit bool
}

func reportTaskDone(state TaskState, taskId int, workerId int) (bool, bool) {
	args := ReportTaskArgs{
		workerId: workerId,
		state:    state,
		taskId:   taskId,
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
