package mr

import (
	"fmt"
	"os/exec"

	//"go/types"
	//"fmt"
	//"bytes"
	//"io/ioutil"
	"log"
	//"path/filepath"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//import "sync/atomic"

var ops int = 0

func initTask(filename string, typo int, nReduce int) *TaskInfo {
	task := TaskInfo{Filename: filename, Typo: typo, nReduce: nReduce}
	task.status = tPending
	task.done = false
	task.ID = ops
	ops++
	return &task
}

func (m *Master) findTask(task *TaskInfo) *TaskInfo {
	for _, t := range m.TaskQueue {
		if t.ID == task.ID {
			return t
		}
	}
	return nil
}

func (m *Master) TaskSuccess(args *TaskInfo, reply *TaskInfo) error {
	//fmt.Printf("接收到成功信号, ID: %d\n", args.ID)
	task := m.findTask(args)
	//fmt.Printf("寻找到本地对象, ID: %d\n", task.ID)
	task.status = tSuccess
	task.done = true
	alldone := true
	for _, t := range m.TaskQueue {
		if !t.done {
			alldone = false
			break
		}
	}
	if alldone && m.Status == mMaping {
		//fmt.Printf("master 状态转换为 mapend %d------\n", mMapend)
		m.Status = mMapend
		//fmt.Printf("map is done!\n")
		m.makeReduceTask()
		return nil
	}
	if alldone && m.Status == mReducing {
		m.Status = mDone
		//fmt.Printf("reduce is done!\n")
		return nil
	}
	return nil
}

func (m *Master) TaskFail(args *TaskInfo, reply *TaskInfo) error {
	task := m.findTask(args)
	task.status = tFailure
	task.done = false
	return nil
}

func newMapTask(filename string, nReduce int) *TaskInfo {
	task := initTask(filename, MapTaskType, nReduce)
	return task
}

func newReduceTask(filename string, nReduce int) *TaskInfo {
	task := initTask(filename, ReduceTaskType, nReduce)
	return task
}

func (m *Master) GetTask(args *TaskInfo, reply *TaskInfo) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, task := range m.TaskQueue {
		if task.status == tPending || task.status == tTimeout || task.status == tFailure {
			task.status = tRunning
			reply.ID = task.ID
			reply.Filename = task.Filename
			reply.nReduce = m.NReduce
			reply.Typo = task.Typo
			reply.Index = task.Index
			//fmt.Printf("给出任务类型%d, 文件名%s \n", reply.Typo, reply.Filename)
			return nil
		}
	}
	reply.ID = tNull
	return nil
}

func (m *Master) GetMasterInfo(args *MasterInfo, reply *MasterInfo) error {
	reply.Status = m.Status
	reply.NReduce = m.NReduce
	return nil
}

type MapTask struct {
	TaskInfo
}

type ReduceTask struct {
	TaskInfo
}

type Master struct {
	// Your definitions here.
	mutex sync.Mutex
	//status    MasterStatus
	TaskQueue []*TaskInfo
	//nReduce   int
	MasterInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//func (m *Master) Debug(args *ExampleArgs, reply *TaskInfo) error {
//	reply.Filename = "('ABDEL')"
//	reply.nReduce = 12
//	m.NReduce = 1000
//	t := newMapTask("BBQ")
//	t.nReduce = 100
//	reply = t
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.Status == mDone {
		return true
	}
	// Your code here.
	//m.status = mDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func (m *Master) makeReduceTask() error {
	i := 0
	m.TaskQueue = []*TaskInfo{}
	for i < m.NReduce {
		filename := fmt.Sprintf("mr-tmp-*-%d", i)
		task := newReduceTask(filename, m.NReduce)
		task.Index = i
		m.TaskQueue = append(m.TaskQueue, task)
		i++
	}
	m.Status = mReducing
	return nil
}

func MakeMaster(files []string, nReduce int) *Master {
	exec.Command("rm", "mr-tmp*")
	exec.Command("rm", "mr-out*")
	m := Master{}
	m.NReduce = nReduce
	m.Status = mIdle
	for _, file := range files {
		mapTask := newMapTask(file, nReduce)
		m.TaskQueue = append(m.TaskQueue, mapTask)
	}
	//fmt.Printf("共有%d个任务", len(m.TaskQueue))
	// go func() error {
	// 	for true {
	// 		//fmt.Printf("当前master状态为%d\n", m.Status)
	// 		success := 0
	// 		for _, i := range m.TaskQueue {
	// 			if i.status == tSuccess {
	// 				success += 1
	// 			}
	// 		}
	// 		//fmt.Printf("成功任务数量%d", success)
	// 		if m.Status == mDone {
	// 			break
	// 		}
	// 		time.Sleep(time.Second)
	// 		time.Sleep(time.Second)
	// 		time.Sleep(time.Second)
	// 		time.Sleep(time.Second)
	// 		time.Sleep(time.Second)
	// 	}
	// 	return nil
	// }()
	m.Status = mMaping
	m.server()
	return &m
}
