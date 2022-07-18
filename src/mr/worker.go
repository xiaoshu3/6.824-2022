package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

//
// main/mrworker.go calls this function.
//
// var p = fmt.Println

var nTask NumsTaskReplay

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	args := EmptyAges{}
	nTask = NumsTaskReplay{}
	call("Coordinator.Numtask", &args, &nTask)
	// p(nTask)

	for {
		taskReply := AskTaskReply{}
		call("Coordinator.AskATask", &args, &taskReply)

		if !taskReply.Valid {
			break
		}

		// max := big.NewInt(1000)
		// rr, _ := rand.Int(rand.Reader, max)
		// if rr.Int64() <= 500 {
		// 	continue
		// }

		if taskReply.TaskType == MAP {

			MapWork(taskReply.TaskNum, mapf)
			args := TaskNumAges{taskReply.TaskType, taskReply.TaskNum}
			emptyArgs := EmptyAges{}
			call("Coordinator.CommpledAMap", &args, &emptyArgs)
		} else if taskReply.TaskType == REDUCE {
			ReduceWork(taskReply.TaskNum, reducef)
			args := TaskNumAges{taskReply.TaskType, taskReply.TaskNum}
			emptyArgs := EmptyAges{}
			call("Coordinator.CommpledAMap", &args, &emptyArgs)
		} else {
			log.Fatal("task type error", taskReply.TaskType)
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func MapWork(num int, mapf func(string, string) []KeyValue) {
	filename := nTask.FileName[num]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	files := []*os.File{}

	for i := 0; i < nTask.NReduce; i++ {
		tmpfile, err := ioutil.TempFile(".", "")
		if err != nil {
			log.Fatal("Cannot create temporary file", err)
		}
		files = append(files, tmpfile)
	}

	// sort.Sort(ByKey(kva))
	for _, kv := range kva {
		n := ihash(kv.Key) % nTask.NReduce
		enc := json.NewEncoder(files[n])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Encode error", err)
		}
	}

	for i := 0; i < nTask.NReduce; i++ {
		os.Rename(files[i].Name(), "mr-"+strconv.Itoa(num)+"-"+strconv.Itoa(i))
		files[i].Close()
	}
}

func ReduceWork(num int, reducef func(string, []string) string) {
	filename := "mr-out-" + strconv.Itoa(num)
	_, err := os.Stat(filename)
	if err == nil {
		return
	}
	if os.IsNotExist(err) {
		intermediate := []KeyValue{}
		for i := 0; i < nTask.NMap; i++ {
			intermidleFile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(num)
			file, _ := os.Open(intermidleFile)

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
		sort.Sort(ByKey(intermediate))

		ofile, err := ioutil.TempFile(".", "")
		if err != nil {
			log.Fatal("Cannot create temporary file", err)
		}
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		os.Rename(ofile.Name(), filename)
		ofile.Close()

	} else {
		log.Fatal("file exist error", filename, err)
	}
}
