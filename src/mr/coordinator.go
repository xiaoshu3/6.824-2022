package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE = iota
	PROGRESS
	COMPLED
)

const (
	MAP = iota
	REDUCE
)

type task struct {
	typeT int
	n     int
	state int
}

type taskQueue struct {
	muQueue         sync.Mutex
	mapTaskQueue    []int
	reduceTaskQueue []int
}

type taskState struct {
	muState         sync.Mutex
	mapTaskState    []task
	reduceTaskState []task
	nMapDone        int
	nReduceDone     int
}

type Coordinator struct {
	// Your definitions here.
	nMap     int
	nReduce  int
	fileName []string
	taskQ    taskQueue
	taskS    taskState
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.taskS.muState.Lock()
	defer c.taskS.muState.Unlock()
	if c.taskS.nMapDone == c.nMap && c.taskS.nReduceDone == c.nReduce {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

var c *Coordinator

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c = &Coordinator{}
	nFile := len(files)
	c.nMap = nFile
	c.nReduce = nReduce
	c.fileName = make([]string, nFile)
	copy(c.fileName, files)

	c.taskQ.mapTaskQueue = make([]int, nFile)
	c.taskS.mapTaskState = make([]task, nFile)
	for i := 0; i < c.nMap; i++ {
		c.taskQ.mapTaskQueue[i] = i
		c.taskS.mapTaskState[i].n = i
	}

	c.taskQ.reduceTaskQueue = make([]int, nReduce)
	c.taskS.reduceTaskState = make([]task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.taskQ.reduceTaskQueue[i] = i
		c.taskS.reduceTaskState[i].n = i
		c.taskS.reduceTaskState[i].typeT = REDUCE
	}
	// Your code here.

	c.server()
	return c
}

func (t *Coordinator) Numtask(args *EmptyAges, reply *NumsTaskReplay) error {
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	reply.FileName = make([]string, c.nMap)
	copy(reply.FileName, c.fileName)

	return nil
}

func (t *Coordinator) AskATask(args *ExampleArgs, reply *AskTaskReply) error {

	for !c.Done() {
		c.taskQ.muQueue.Lock()
		c.taskS.muState.Lock()
		if len(c.taskQ.mapTaskQueue) > 0 {
			num := c.taskQ.mapTaskQueue[0]
			c.taskQ.mapTaskQueue = c.taskQ.mapTaskQueue[1:]

			if c.taskS.mapTaskState[num].state == COMPLED {

			} else if c.taskS.mapTaskState[num].state == IDLE {
				c.taskS.mapTaskState[num].state = PROGRESS
				reply.TaskNum = num
				reply.TaskType = MAP
				reply.Valid = true
				go c.expireCheck(num, MAP)
				c.taskS.muState.Unlock()
				c.taskQ.muQueue.Unlock()
				return nil
			} else {
				log.Println("PROGRESS...")
			}
		}

		if len(c.taskQ.reduceTaskQueue) > 0 {
			if c.taskS.nMapDone == c.nMap {
				num := c.taskQ.reduceTaskQueue[0]
				c.taskQ.reduceTaskQueue = c.taskQ.reduceTaskQueue[1:]
				if c.taskS.reduceTaskState[num].state == IDLE {
					c.taskS.reduceTaskState[num].state = PROGRESS
					reply.TaskNum = num
					reply.TaskType = REDUCE
					reply.Valid = true
					go c.expireCheck(num, REDUCE)
					c.taskS.muState.Unlock()
					c.taskQ.muQueue.Unlock()
					return nil
				} else {
					log.Println(num, c.taskS.reduceTaskState[num].state)
				}
			}
		}
		c.taskS.muState.Unlock()
		c.taskQ.muQueue.Unlock()

		time.Sleep(time.Second / 2)
	}
	reply.Valid = false
	return nil

}

func (t *Coordinator) expireCheck(taskNum, taskType int) {
	time.Sleep(10 * time.Second)
	c.taskQ.muQueue.Lock()
	c.taskS.muState.Lock()
	defer c.taskS.muState.Unlock()
	defer c.taskQ.muQueue.Unlock()
	if taskType == MAP {
		if c.taskS.mapTaskState[taskNum].state != COMPLED {
			c.taskS.mapTaskState[taskNum].state = IDLE
			c.taskQ.mapTaskQueue = append(c.taskQ.mapTaskQueue, taskNum)
		}
	} else {
		if c.taskS.reduceTaskState[taskNum].state != COMPLED {
			c.taskS.reduceTaskState[taskNum].state = IDLE
			c.taskQ.reduceTaskQueue = append(c.taskQ.reduceTaskQueue, taskNum)
		}
	}
}

func (t *Coordinator) MapDone(args *EmptyAges, reply *BoolReply) error {
	c.taskS.muState.Lock()
	n := c.taskS.nMapDone
	c.taskS.muState.Unlock()
	reply.Boolre = (n == c.nMap)
	return nil
}

func (t *Coordinator) CommpledAMap(args *TaskNumAges, reply *EmptyAges) error {
	c.taskS.muState.Lock()
	defer c.taskS.muState.Unlock()
	n := args.TaskNum
	if args.TaskT == MAP {
		c.taskS.nMapDone++
		c.taskS.mapTaskState[n].state = COMPLED
	} else if args.TaskT == REDUCE {
		c.taskS.nReduceDone++
		c.taskS.reduceTaskState[n].state = COMPLED
	} else {
		return errors.New("task type error %d")
	}

	return nil
}
