package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	//"os/exec"
	"strconv"
	"sync"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/porcupine"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is less than 3 endpoints fail
	if flag.NArg() < 3 {
		flag.Usage()
		os.Exit(1)
	}

	endpoints := make([]string, flag.NArg())
	kvs := make([]pb.KvStoreClient, flag.NArg())
	for i, _ := range endpoints {
		endpoints[i] = flag.Args()[i]
		log.Printf("Connecting to %v", endpoints[i])
		// Connect to the server. We use WithInsecure since we do not configure https in this class.
		conn, err := grpc.Dial(endpoints[i], grpc.WithInsecure())
		//Ensure connection did not fail.
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v with error %v", endpoints[i], err)
		}
		log.Printf("Connected")
		// Create a KvStore client
		kvs[i] = pb.NewKvStoreClient(conn)
	}

	var cid int64
	var wg sync.WaitGroup

	type registerInput struct {
		op    bool // false = write, true = read
		value string
	}

	// a sequential specification of a register
	registerModel := porcupine.Model{
		Init: func() interface{} {
			return ""
		},
		// step function: takes a state, input, and output, and returns whether it
		// was a legal operation, along with a new state
		Step: func(state, input, output interface{}) (bool, interface{}) {
			regInput := input.(registerInput)
			if regInput.op == false {
				return true, regInput.value // always ok to execute a write
			} else {
				readCorrectValue := output == state
				return readCorrectValue, state // state is unchanged
			}
		},
	}

	events := []porcupine.Event{}
	reqres := [200]int{}
	//var cmd1 *exec.Cmd
	//var cmd2 *exec.Cmd

	for n := 0; n < 100; n++ {

		cid++
		getorset := rand.Intn(2)
		var value string
		if getorset == 1 {
			value = strconv.Itoa(rand.Intn(10))
		}

		getReq := &pb.Key{Key: "x", Cid: cid}
		putReq := &pb.KeyValue{Key: "x", Value: value, Cid: cid}

		for i, _ := range kvs {
			wg.Add(1)
			go func(idx int, tid uint, gr *pb.Key, pr *pb.KeyValue, ncmd int) {
				reqres[2*ncmd]++
				if reqres[2*ncmd] == 1 {
					if pr.Value == "" {
						// get
						events = append(events, porcupine.Event{Kind: porcupine.CallEvent, Value: registerInput{true, ""}, Id: tid - 1})

					} else {
						// set
						events = append(events, porcupine.Event{Kind: porcupine.CallEvent, Value: registerInput{false, pr.Value}, Id: tid - 1})
					}
				}

				if pr.Value != "" {
					// set
					res, err := kvs[idx].Set(context.Background(), pr)
					if err != nil {
						log.Printf("Replica %v was likely killed", idx)
						wg.Done()
						return
					}
					if res.Cid != 0 {
						reqres[2*ncmd+1]++
						if reqres[2*ncmd+1] == 1 {
							events = append(events, porcupine.Event{Kind: porcupine.ReturnEvent, Value: "", Id: tid - 1})
						}

						log.Printf("Got response replica: \"%v\" cid: \"%v\" key: \"%v\" value:\"%v\"", idx, res.Cid, res.GetKv().Key, res.GetKv().Value)
						if res.GetKv().Key != "x" {
							log.Printf("Put returned the wrong response")
						}
					} else {
						log.Printf("Got decision from replica %v before request sent cid %v", idx, res.Cid)
					}

				} else {
					// get
					res, err := kvs[idx].Get(context.Background(), gr)
					if err != nil {
						log.Printf("Replica %v was likely killed", idx)
						wg.Done()
						return
					}
					if res.Cid != 0 {
						reqres[2*ncmd+1]++
						if reqres[2*ncmd+1] == 1 {
							events = append(events, porcupine.Event{Kind: porcupine.ReturnEvent, Value: res.GetKv().Value, Id: tid - 1})
						}
						log.Printf("Got response replica: \"%v\" cid: \"%v\" key: \"%v\" value:\"%v\"", idx, res.Cid, res.GetKv().Key, res.GetKv().Value)
						if res.GetKv().Key != "x" {
							log.Printf("Get returned the wrong response")
						}
					} else {
						log.Printf("Got decision from replica %v before request sent cid %v", idx, res.Cid)
					}

				}
				wg.Done()
			}(i, uint(cid), getReq, putReq, n)

		}

		/*
			if n == 4 {
				cmd1 = exec.Command("../launch-tool/launch.py", "kill", "2")
				log.Printf("Running kill 2 and waiting for it to finish...")
				err1 := cmd1.Start()
				if err1 != nil {
					log.Printf("err1: %v", err1)
				}
			}
			if n == 6 {

				cmd2 = exec.Command("../launch-tool/launch.py", "kill", "4")
				log.Printf("Running kill 4 and waiting for it to finish...")
				err2 := cmd2.Start()
				if err2 != nil {
					log.Printf("err2: %v", err2)
				}
			}
		*/
	}

	//err1 := cmd1.Wait()
	//err2 := cmd2.Wait()
	//log.Printf("Kill finished with err : %v", err1)
	//log.Printf("Kill finished with err : %v", err2)

	//wg.Wait()

	if waitTimeout(&wg, 25*time.Second) {
		log.Println("Timed out waiting for wait group")
		ok := porcupine.CheckEvents(registerModel, events)
		log.Printf("linearizability %v", ok)
	} else {
		log.Println("Wait group finished")
		ok := porcupine.CheckEvents(registerModel, events)
		log.Printf("linearizability %v", ok)
	}

}
