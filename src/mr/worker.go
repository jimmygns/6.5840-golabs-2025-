package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Get process ID for worker
func GetProcessID() int {
	return os.Getpid()
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Printf("Worker %d started\n", GetProcessID())
	// Your worker implementation here.
	for {
		mapTaskReply := RequestMapTask()
		f := mapTaskReply.FileName
		nReduce := mapTaskReply.NReduce
		mapTaskID := mapTaskReply.TaskID
		if f != "" {
			fmt.Printf("Map Task %s\n", f)
			file, err := os.Open(f)
			if err != nil {
				log.Fatalf("cannot open %v", f)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", f)
			}
			file.Close()
			kva := mapf(f, string(content))
			// Partition the intermediate data into NReduce buckets
			intermediate := make([][]KeyValue, nReduce)
			for _, kv := range kva {
				idx := ihash(kv.Key) % nReduce
				intermediate[idx] = append(intermediate[idx], kv)
			}
			// Write intermediate data to files
			for rtid, kv := range intermediate {
				tempFileName := fmt.Sprintf("mr-out-%d-%d", mapTaskID, rtid)
				tempFile, err := os.CreateTemp(".", fmt.Sprintf("%s-*", tempFileName))
				if err != nil {
					fmt.Printf("Cannot create temp file %s\n", tempFileName)
					return
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range kv {
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Printf("Cannot encode kv %v\n", kv)
						return
					}
				}
				if err := tempFile.Close(); err != nil {
					fmt.Printf("Cannot close temp file %s\n", tempFileName)
					return
				}
				if err := os.Rename(tempFile.Name(), tempFileName); err != nil {
					fmt.Printf("Cannot rename temp file %s\n", tempFileName)
					return
				}
				if ok := RegisterReducerTask(rtid, tempFileName); !ok {
					fmt.Println("Register Reducer Task Failed")
					return
				}
			}
			FinishTask(mapTaskID, Map)
		} else {
			// Ask for reduce task
			reduceTaskReply := RequestReduceTask()
			if len(reduceTaskReply.FileNames) != 0 {
				reduceTaskID := reduceTaskReply.TaskID

				// Read intermediate data from files
				kva := []KeyValue{}
				for _, f := range reduceTaskReply.FileNames {
					file, err := os.Open(f)
					if err != nil {
						log.Fatalf("cannot open %v", f)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err == io.EOF {
							break
						} else if err != nil {
							log.Fatalf("cannot decode %v", f)
						}
						kva = append(kva, kv)
					}
					file.Close()
				}
				// Sort intermediate data
				sort.Sort(ByKey(kva))
				oname := fmt.Sprintf("mr-out-%d", reduceTaskID)
				tempFile, err := os.CreateTemp(".", fmt.Sprintf("%s-*", oname))
				if err != nil {
					fmt.Printf("Cannot create temp file %s\n", oname)
					return
				}
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)
					fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				if err := tempFile.Close(); err != nil {
					fmt.Printf("Cannot close temp file %s\n", oname)
					return
				}
				if err := os.Rename(tempFile.Name(), oname); err != nil {
					fmt.Printf("Cannot rename temp file %s\n", oname)
					return
				}
				FinishTask(reduceTaskID, Reduce)
			}
		}
	}
}

func FinishTask(taskID int, taskType TaskType) {
	args := UpdateTaskArgs{
		TaskID:   taskID,
		Status:   Completed,
		TaskType: taskType,
	}
	reply := UpdateTaskReply{}
	ok := call("Coordinator.UpdateTask", &args, &reply)
	if !ok {
		fmt.Println("Update Task Failed")
	}
}

func RegisterReducerTask(rtid int, filename string) bool {
	args := RegisterReducerTaskArgs{
		WorkerPID: GetProcessID(),
		TaskID:    rtid,
		Filename:  filename,
	}
	reply := RegisterReducerTaskReply{}
	ok := call("Coordinator.RegisterReducerTask", &args, &reply)
	if !ok {
		fmt.Println("Register Reducer Failed")
	}
	return ok
}

func RequestMapTask() MapTaskReply {
	args := MapTaskArgs{
		WorkerPID: GetProcessID(),
	}
	reply := MapTaskReply{}
	ok := call("Coordinator.MapTask", &args, &reply)
	if ok {
		return reply
	}
	return reply
}

func RequestReduceTask() ReduceTaskReply {
	args := ReduceTaskArgs{
		WorkerPID: GetProcessID(),
	}
	reply := ReduceTaskReply{}
	ok := call("Coordinator.ReduceTask", &args, &reply)
	if ok {
		return reply
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	// fmt.Printf("worker %d: rpcname: %s\n", GetProcessID(), rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	os.Exit(0)
	return false
}
