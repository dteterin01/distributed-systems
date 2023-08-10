package mr

import (
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TempDir = "tmp"
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
	file func(idx int, files []string) string,
	appender func(*Master, Task)) {

	for i := 0; i < count; i++ {
		mTask := builder(i, file(i, files))
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

func (m *Master) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	reply.ReduceCount = len(m.reduceTasks)

	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mtx.Lock()
	var task *Task
	if m.nMap > 0 {
		task = m.findNextTaskForJob(m.mapTasks, args.WorkerId)
	} else if m.nReduce > 0 {
		task = m.findNextTaskForJob(m.reduceTasks, args.WorkerId)
	} else {
		task = &Task{ExitState, Finish, -1, "", -1}
	}

	reply.TaskId = task.idx
	reply.TaskFile = task.file
	reply.State = task.state
	reply.NReduce = m.nReduce

	m.mtx.Unlock()

	go m.waitForTask(task)
	return nil
}

func (m *Master) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var task *Task
	if args.State == MapState {
		task = &m.mapTasks[args.TaskId]
	} else if args.State == ReduceState {
		task = &m.reduceTasks[args.TaskId]
	} else {
		return nil
	}

	if args.WorkerId == task.workerId && task.status == Schedule {
		task.status = Finish
		if args.State == MapState && m.nMap > 0 {
			m.nMap--
		} else if args.State == ReduceState && m.nReduce > 0 {
			m.nReduce--
		}
	}

	reply.CanExit = m.nMap == 0 && m.nReduce == 0
	return nil
}

// build master

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	nMap := len(files)

	m.nReduce = nReduce
	m.nMap = nMap

	m.mapTasks = make([]Task, 0, nMap)
	m.reduceTasks = make([]Task, 0, nReduce)

	m.preInitTasks(nMap, files, createMapTask,
		func(idx int, files []string) string {
			return files[idx]
		},
		func(m *Master, task Task) {
			m.mapTasks = append(m.mapTasks, task)
		})

	m.preInitTasks(nReduce, files, createReduceTask,
		func(idx int, files []string) string {
			return ""
		},
		func(m *Master, task Task) {
			m.reduceTasks = append(m.reduceTasks, task)
		})

	m.server()

	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &m
}
