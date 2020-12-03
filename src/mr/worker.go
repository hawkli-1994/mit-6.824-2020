package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	//"math/rand"
	"os"
	"path/filepath"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"
//
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkError struct {
	Err string
}

func (w WorkError) Error() string {
	panic(w.Err)
}


type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



func doMap(task *TaskInfo, mapf func(string, string) []KeyValue) error {
		if task.status == tNull {
			return nil
		} else {
			file, err := os.Open(task.Filename)
			defer file.Close()
			if err != nil {
				log.Fatalf("can not open %d", task.ID)
				log.Fatalf("can not open %v", task.Filename)
				return err
			}
			content, err := ioutil.ReadAll(file)
			file.Close()
			if err != nil {
				file.Close()
				log.Fatalf("cannot read %v", task.Filename)
				return err
			}

			intermediate := mapf(task.Filename, string(content))
			tmp := make(map[string][]KeyValue)
			for _, kva := range intermediate {
				key := kva.Key
				_, inTmp := tmp[key]
				if !inTmp {
					var kvl []KeyValue
					tmp[key] = kvl
				}
				tmp[key] = append(tmp[key], kva)
			}
			for key, kvl := range tmp {
				idx := ihash(key) % task.nReduce
				tmpfile, err := ioutil.TempFile(".", "mr-tmp2-*")
				if err != nil {
					return err
				}
				encoder := json.NewEncoder(tmpfile)
				encoder.Encode(kvl)
				filename := fmt.Sprintf("mr-tmp-%d-%d", task.ID, idx)
				os.Rename(tmpfile.Name(), filename)
				tmpfile.Close()
			}
			call("Master.TaskSuccess", &task, &task)
			fmt.Printf("map %s 成功\n", task.Filename)
			return nil
		}
}
//

func doReduce(task *TaskInfo, reducef func(string, []string) string) error {
	call("Master.GetTask", task, task)
	if task.ID == tNull {
		return nil
	}
	if task.Typo != ReduceTaskType {
		return nil
	}
	files, err := filepath.Glob(task.Filename)
	if err != nil {
		return err
	}
	var intermediate []KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		tmp := []KeyValue{}
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&tmp)
		if err != nil {
			return err
		}
		file.Close()
		intermediate = append(intermediate, tmp...)
	}
	sort.Sort(ByKey(intermediate))
	reduceArgs := make(map[string][]string)
	for _, item := range intermediate {
		_, inArgs := reduceArgs[item.Key]
		if !inArgs {
			reduceArgs[item.Key] = []string{}
		}
		reduceArgs[item.Key] = append(reduceArgs[item.Key], item.Value)
	}
	tmpfile, err := ioutil.TempFile(".", "mr-tmpre-*")
	if err != nil {
		return err
	}
	//encoder := json.NewEncoder(tmpfile)
	defer tmpfile.Close()
	//overout := ""
	for k, v :=  range reduceArgs {
		output := reducef(k, v)
		fmt.Fprintf(tmpfile, "%v %v\n", k, output)
		//overout = overout + "\n" + output
	}

	//encoder.Encode(overout)
	filename := fmt.Sprintf("mr-out-%d", task.Index)
	if err != nil {
		return err
	}
	os.Rename(tmpfile.Name(), filename)
	call("Master.TaskSuccess", &task, &task)
	fmt.Printf("reduce %s成功\n", task.Filename)
	return nil
}
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		masterInfo := MasterInfo{}
		if masterInfo.Status == mDone {
			os.Exit(0)
		}
		call("Master.GetMasterInfo", &masterInfo, &masterInfo)
		if masterInfo.Status == mIdle || masterInfo.Status == mMaping {
			mapTask := TaskInfo{nReduce:masterInfo.NReduce}
			call("Master.GetTask", &mapTask, &mapTask)
			if mapTask.ID == tNull {
				continue
			}
			if mapTask.Typo != MapTaskType {
				continue
			}
			err := doMap(&mapTask, mapf)
			if err != nil {
				fmt.Printf("err: %s", err)
				call("TaskFail", &mapTask, &mapTask)
			}
		} else if masterInfo.Status == mReducing {
			reduceTask := TaskInfo{}
			err := doReduce(&reduceTask, reducef)
			if err != nil {
				fmt.Printf("err: %s", err)
				call("TaskFail", &reduceTask, &reduceTask)
			}
		} else if masterInfo.Status == mDone {
			break
		}
		//time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
