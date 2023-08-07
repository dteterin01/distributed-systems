package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const taskTimeout = 10 // seconds

type JobStage int
type TaskState int
type TaskStatus int

const (
	MapState TaskState = iota
	ReduceState
	NoTask
	ExitState
)

const (
	NotStarted TaskStatus = iota
	Schedule
	Finish
)

type Task struct {
	state    TaskState
	status   TaskStatus
	idx      int
	file     string
	workerId int // workers pid
}

type Master struct {
	// Your definitions here.
	mtx sync.Mutex

	// preinited jobs
	mapTasks    []Task
	reduceTasks []Task

	nMap    int // count of Map stage task equals count of files
	nReduce int // count of reduce stage task
}

func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) Done() bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.nMap == 0 && m.nReduce == 0
}

func (m *Master) preInitTasks(
	count int,
	files []string,
	builder func(int, string) Task,
	appender func(*Master, Task)) {

	for i := 0; i < count; i++ {
		mTask := builder(i, files[i])
		appender(m, mTask)
	}
}

func createMapTask(idx int, file string) Task {
	return Task{
		state:    MapState,
		status:   NotStarted,
		idx:      idx,
		file:     file,
		workerId: -1,
	}
}

func createReduceTask(idx int, _ string) Task {
	return Task{
		state:    ReduceState,
		status:   NotStarted,
		idx:      idx,
		file:     "",
		workerId: -1,
	}
}

// logic:

func (m *Master) findNextTaskForJob(taskList []Task, workerId int) *Task {
	var task *Task

	for i := 0; i < len(taskList); i++ {
		if taskList[i].status == NotStarted {
			task = &taskList[i]
			task.status = Schedule
			task.workerId = workerId
			return task
		}
	}

	return &Task{NoTask, Finish, -1, "", -1}

}

func (m *Master) waitForTask(task *Task) {
	if task.state != MapState && task.state != ReduceState {
		return
	}

	<-time.After(time.Second * taskTimeout)

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if task.status == Schedule {
		task.status = NotStarted
		task.workerId = -1
	}
}

// Rpc handlers

func (m *Master) RequestTask(args *RequestTask, reply *RequestTaskReply) {
	m.mtx.Lock()
	var task *Task
	if m.nMap > 0 {
		task = m.findNextTaskForJob(m.mapTasks, args.workerId)
	} else if m.nReduce > 0 {
		task = m.findNextTaskForJob(m.reduceTasks, args.workerId)
	} else {
		task = &Task{ExitState, Finish, -1, "", -1}
	}

	reply.taskId = task.idx
	reply.taskFile = task.file
	reply.state = task.state
	reply.nReduce = m.nReduce

	m.mtx.Unlock()

	go m.waitForTask(task)
}

func (m *Master) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var task *Task
	if args.state == MapState {
		task = &m.mapTasks[args.workerId]
	} else if args.state == ReduceState {
		task = &m.reduceTasks[args.workerId]
	} else {
		return
	}

	if args.workerId == task.workerId && task.status == Schedule {
		task.status = Finish
		if args.state == MapState && m.nMap > 0 {
			m.nMap--
		} else if args.state == ReduceState && m.nReduce > 0 {
			m.nReduce--
		}
	}

	reply.CanExit = m.nMap == 0 && m.nReduce == 0
}

// build master

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	nMap := len(files)

	m.nReduce = nReduce
	m.nMap = nMap

	m.preInitTasks(nMap, files, createMapTask,
		func(m *Master, task Task) {
			m.mapTasks = append(m.mapTasks, task)
		})

	m.preInitTasks(nReduce, files, createReduceTask,
		func(m *Master, task Task) {
			m.reduceTasks = append(m.reduceTasks, task)
		})

	m.server()
	return &m
}
