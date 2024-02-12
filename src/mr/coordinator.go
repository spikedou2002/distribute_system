package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	//lock sync.Mutex // 保护共享信息，避免并发冲突

	stage       int        // 1 for map, 2 for reduce 3 for waiting
	MapChan     chan *Task //channel for map functions
	Maptasks    []*Task    //array of map tasks
	ReduceChan  chan *Task //channel for reduce tasks
	Reducetasks []*Task    //array of reduce tasks
	RemainTasks int        // #of remain tasks
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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
func (c *Coordinator) PostTask(args *ExampleArgs, reply *Task) error {
	mutex.Lock()
	defer mutex.Unlock()
	if c.stage == Map {
		if len(c.MapChan) > 0 {
			*reply = *<-c.MapChan
			c.Maptasks[reply.TaskId].Start = time.Now()
		} else {
			if args.X != Waiting {
				reply.TaskType = Waiting
			} else {
				i := 0
				for i < len(c.Maptasks) {
					if c.Maptasks[i].Finished == false && time.Since(c.Maptasks[i].Start) > time.Second*15 {
						*reply = *c.Maptasks[i]
						c.Maptasks[i].Start = time.Now()
						break
					}
					i++
				}
			}
		}
	} else if c.stage == Reduce {
		if len(c.ReduceChan) > 0 {
			*reply = *<-c.ReduceChan
			c.Reducetasks[reply.TaskId].Start = time.Now()
		} else {
			if args.X != Waiting {
				reply.TaskType = Waiting
			} else {
				i := 0
				for i < len(c.Reducetasks) {
					if c.Reducetasks[i].Finished == false && time.Since(c.Reducetasks[i].Start) > time.Second*15 {
						*reply = *c.Reducetasks[i]
						c.Reducetasks[i].Start = time.Now()
						break
					}
					i++
				}

			}
		}
	} else {
		reply.TaskType = 0
	}
	return nil
}
func (c *Coordinator) FinishTask(task *Task, reply *ExampleReply) error {
	if task.TaskType == Map {
		mutex.Lock()
		if c.Maptasks[task.TaskId].Finished == false {
			c.Maptasks[task.TaskId].Finished = true
			c.RemainTasks--
		}
		mutex.Unlock()
	} else if task.TaskType == Reduce {
		mutex.Lock()
		if c.Reducetasks[task.TaskId].Finished == false {
			c.Reducetasks[task.TaskId].Finished = true
			c.RemainTasks--
		}
		mutex.Unlock()
	}
	if c.Check(task.NReduce) {
		c.stage = Waiting
	}

	return nil
}
func (c *Coordinator) Check(Nreduce int) bool {
	if c.RemainTasks == 0 {
		if len(c.Reducetasks) == 0 {
			c.CreateReduce(Nreduce)
			return false
		}
		return true
	} else {

	}
	return false
}
func (c *Coordinator) CreateReduce(Nreduce int) {
	i := 0
	for i < Nreduce {
		task := Task{
			TaskType: Reduce,
			Filename: "mr-",
			NReduce:  len(c.Maptasks),
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		i++
		c.ReduceChan <- &task
		c.Reducetasks = append(c.Reducetasks, &task)
	}
	c.stage = Reduce
	c.RemainTasks = Nreduce
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := (c.stage == Waiting)

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:       Map,
		MapChan:     make(chan *Task, len(files)),
		Maptasks:    make([]*Task, len(files)),
		Reducetasks: make([]*Task, 0),
		ReduceChan:  make(chan *Task, nReduce),
		RemainTasks: len(files),
	}
	i := 0
	for _, file := range files {
		task := Task{
			TaskType: Map,
			Filename: file,
			NReduce:  nReduce,
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		c.MapChan <- &task
		c.Maptasks[i] = &task
		i++
	}

	// Your code here.

	c.server()
	return &c
}
