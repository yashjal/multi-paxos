package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
)

// Messages that can be passed from the Paxos RPC server to the main loop for Accept
type AcceptInput struct {
	arg      *pb.P2A
	response chan pb.P2B
}

// Messages that can be passed from the Paxos RPC server to the main loop for Prepare
type PrepareInput struct {
	arg      *pb.P1A
	response chan pb.P1B
}

// Messages that can be passed from the Paxos RPC server to the main loop for Propose
type ProposeInput struct {
	arg      *pb.SlotCmd
	response chan pb.Empty
}

// Messages that can be passed from the Paxos RPC server to the main loop for Decision
type DecisionInput struct {
	arg      *pb.SlotCmd
	response chan pb.Empty
}

// Struct off of which we shall hang the Paxos service
type Paxos struct {
	PrepareChan  chan PrepareInput
	AcceptChan   chan AcceptInput
	ProposeChan  chan ProposeInput
	DecisionChan chan DecisionInput
}

func (r *Paxos) Prepare(ctx context.Context, arg *pb.P1A) (*pb.P1B, error) {
	c := make(chan pb.P1B)
	r.PrepareChan <- PrepareInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Paxos) Accept(ctx context.Context, arg *pb.P2A) (*pb.P2B, error) {
	c := make(chan pb.P2B)
	r.AcceptChan <- AcceptInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Paxos) Propose(ctx context.Context, arg *pb.SlotCmd) (*pb.Empty, error) {
	c := make(chan pb.Empty)
	r.ProposeChan <- ProposeInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Paxos) Decision(ctx context.Context, arg *pb.SlotCmd) (*pb.Empty, error) {
	c := make(chan pb.Empty)
	r.DecisionChan <- DecisionInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand, durationMin int, durationMax int) time.Duration {
	return time.Duration(r.Intn(durationMax-durationMin)+durationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand, durationMin int, durationMax int) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		for len(timer.C) > 0 {
			<-timer.C
		}
	}
	timer.Reset(randomDuration(r, durationMin, durationMax))
}

// Launch a GRPC service for this Paxos peer.
func RunPaxosServer(r *Paxos, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterPaxosServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.PaxosClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewPaxosClient(nil), err
	}
	return pb.NewPaxosClient(conn), nil
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	paxos := Paxos{PrepareChan: make(chan PrepareInput), AcceptChan: make(chan AcceptInput), ProposeChan: make(chan ProposeInput), DecisionChan: make(chan DecisionInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunPaxosServer(&paxos, port)

	peerClients := make(map[string]pb.PaxosClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	type PrepareResponse struct {
		ret  *pb.P1B
		err  error
		peer string
	}

	type AcceptResponse struct {
		ret  *pb.P2B
		err  error
		peer string
	}

	prepareResponseChan := make(chan PrepareResponse)
	acceptResponseChan := make(chan AcceptResponse)

	leaders := make(map[string]bool)
	var slotNum, minSlot int64
	replicaProposals := make(map[int64]InputChannelType)
	replicaDecisions := make(map[int64]pb.Command)
	leaderProposals := make(map[int64]pb.Command)
	var acceptorBallotNum int64 = -1
	var acceptorBallotID string
	var myBallotNum int64 = -1
	var accepted []*pb.Pvalue
	var active bool
	cidToSlot := make(map[int64]int64)
	var scoutWaitFor int
	var scoutPvals []*pb.Pvalue
	var commanderWaitFor map[int64]int

	localHandleCommand := func(c pb.Command) pb.Result {
		switch c.Operation {
		case pb.Op_GET:
			arg := c.GetGet()
			return s.GetInternal(arg.Key, c.Cid)
		case pb.Op_SET:
			arg := c.GetSet()
			return s.SetInternal(arg.Key, arg.Value, c.Cid)
		case pb.Op_CLEAR:
			return s.ClearInternal(c.Cid)
		case pb.Op_CAS:
			arg := c.GetCas()
			return s.CasInternal(arg.Kv.Key, arg.Kv.Value, arg.Value.Value, c.Cid)
		default:
			log.Fatalf("Unrecognized operation %v", c)
			return pb.Result{}
		}
	}

	propose := func(ict InputChannelType) int64 {
		cd := ict.command
		for _, v := range replicaDecisions {
			if cd.Cid == v.Cid { // assumes every command has unique cid
				return -1
			}
		}
		var emptySlot int64 = -1
		for i := int64(0); i < minSlot; i++ {
			_, ok1 := replicaProposals[i]
			_, ok2 := replicaDecisions[i]
			if !ok1 && !ok2 {
				emptySlot = i
			}
		}
		var sPrime int64
		if emptySlot == -1 {
			sPrime = minSlot
			minSlot++
		} else {
			sPrime = emptySlot
		}
		replicaProposals[sPrime] = ict
		cidToSlot[cd.Cid] = sPrime
		return sPrime
	}

	perform := func(cd pb.Command) {
		found := false
		log.Printf("perform")
		for i := int64(0); i < slotNum; i++ {
			cd1, ok := replicaDecisions[i]
			if ok && cd.Cid == cd1.Cid {
				slotNum++
				found = true
				break
			}
		}
		// find ict in proposals with cid == cd.Cid
		if !found {
			sn, ok1 := cidToSlot[cd.Cid]
			if ok1 {
				ict := replicaProposals[sn]
				slotNum++
				log.Printf("handling command id %v", cd.Cid)
				s.HandleCommand(ict)
			} else {
				log.Println("Replica has not yet received command from client (probably replica was down), so handle command locally")
				slotNum++
				res := localHandleCommand(cd)
				log.Printf("Response from handling command locally %v", res)

			}
		}
	}

	// Create a timer and start running it
	timer := time.NewTimer(500 * time.Millisecond)

	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// The timer went off.
			log.Printf("Timeout for proposer %v", id)
			scoutWaitFor = len(*peers) + 1
			scoutPvals = []*pb.Pvalue{}
			myBallotNum++
			// proposer sends prepare requests to acceptors
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.PaxosClient, p string) {
					ret, err := c.Prepare(context.Background(), &pb.P1A{BallotID: id, BallotNum: myBallotNum})
					prepareResponseChan <- PrepareResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// send to self
			go func(p string) {
				c := make(chan pb.P1B)
				paxos.PrepareChan <- PrepareInput{arg: &pb.P1A{BallotID: id, BallotNum: myBallotNum}, response: c}
				result := <-c
				prepareResponseChan <- PrepareResponse{ret: &result, err: nil, peer: p}
			}(id)
			//restartTimer(timer, r, )
		case op := <-s.C:
			// replica received an operation from a client
			// no redirects needed in multi-paxos
			log.Printf("Replica %v received command with id %v", id, op.command.Cid)
			sPrime := propose(op)
			if sPrime != -1 {
				for i, v := range leaders {
					if v {
						if i == id {
							go func(p string) {
								c := make(chan pb.Empty)
								paxos.ProposeChan <- ProposeInput{arg: &pb.SlotCmd{SlotNum: sPrime, Cmd: &op.command, Id: id}, response: c}
								result := <-c
								log.Printf("Response to propose %v", result)

							}(i)
						} else {
							go func(c pb.PaxosClient, p string) {
								ret, err := c.Propose(context.Background(), &pb.SlotCmd{SlotNum: sPrime, Cmd: &op.command, Id: id})
								if err != nil {
									log.Printf("Response to propose %v", ret)
								}
							}(peerClients[i], i)
						}
					}
				}
			} else {
				// decision was made before request was sent
				op.response <- pb.Result{}
			}
		case p1a := <-paxos.PrepareChan:
			// acceptor received a prepare request from a paxos proposer
			log.Printf("Acceptor %v received prepare request from proposer %v with ballot number %v", id, p1a.arg.BallotID, p1a.arg.BallotNum)
			if p1a.arg.BallotNum > acceptorBallotNum || (p1a.arg.BallotNum == acceptorBallotNum && p1a.arg.BallotID > acceptorBallotID) {
				acceptorBallotNum = p1a.arg.BallotNum
				acceptorBallotID = p1a.arg.BallotID
			}
			p1a.response <- pb.P1B{BallotID: acceptorBallotID, BallotNum: acceptorBallotNum, Accepted: accepted}
			//if p1a.arg.BallotID != id {
			restartTimer(timer, r, 50*int(myBallotNum)+1000, 50*int(myBallotNum)+4000)
			//}
		case p2a := <-paxos.AcceptChan:
			// acceptor received an accept RPC from a paxos leader
			log.Printf("Acceptor %v received accept request from %v", id, p2a.arg.LeaderID)
			bn := p2a.arg.Pv.BallotNum
			bi := p2a.arg.Pv.BallotID
			sn := p2a.arg.Pv.SlotNum
			log.Printf("Arg Pvalue (b,s,c) <%v, %v>, %v, %v", bn, bi, sn, p2a.arg.Pv.Cmd.Cid)
			if bn > acceptorBallotNum || (bn == acceptorBallotNum && bi >= acceptorBallotID) {
				acceptorBallotNum = bn
				acceptorBallotID = bi
				// don't want duplicates in accepted
				// optimization: keep latest pvalue for each slot
				// check for out of index error
				if int64(len(accepted)) > sn {
					accepted[sn] = p2a.arg.Pv
				} else {
					start := int64(len(accepted))
					for i := start; i < sn; i++ {
						accepted = append(accepted, &pb.Pvalue{})
					}
					accepted = append(accepted, p2a.arg.Pv) // position sn
				}
			}
			p2a.response <- pb.P2B{BallotID: acceptorBallotID, BallotNum: acceptorBallotNum, SlotNum: sn, Cmd: p2a.arg.Pv.Cmd}
			//if p2a.arg.LeaderID != id {
			restartTimer(timer, r, 50*int(myBallotNum)+1000, 50*int(myBallotNum)+4000)
			//}
		case pr := <-paxos.ProposeChan:
			// leader (at the time) received a propose RPC from a replica
			log.Printf("Leader %v received a proposal from replice %v with slot num %v and command id %v", id, pr.arg.Id, pr.arg.SlotNum, pr.arg.Cmd.Cid)
			cd, ok := leaderProposals[pr.arg.SlotNum]
			if !ok || cd.Cid == pr.arg.Cmd.Cid {
				leaderProposals[pr.arg.SlotNum] = *pr.arg.Cmd
				if active {
					commanderWaitFor[pr.arg.SlotNum] = len(*peers) + 1
					// leader sends accept requests to acceptors
					for p, c := range peerClients {
						go func(c pb.PaxosClient, p string) {
							ret, err := c.Accept(context.Background(), &pb.P2A{LeaderID: id, Pv: &pb.Pvalue{BallotID: id, BallotNum: myBallotNum, SlotNum: pr.arg.SlotNum, Cmd: pr.arg.Cmd}})
							acceptResponseChan <- AcceptResponse{ret: ret, err: err, peer: p}
						}(c, p)
					}
					// send to self
					go func(p string) {
						c := make(chan pb.P2B)
						paxos.AcceptChan <- AcceptInput{arg: &pb.P2A{LeaderID: id, Pv: &pb.Pvalue{BallotID: id, BallotNum: myBallotNum, SlotNum: pr.arg.SlotNum, Cmd: pr.arg.Cmd}}, response: c}
						result := <-c
						acceptResponseChan <- AcceptResponse{ret: &result, err: nil, peer: p}
					}(id)
				}
			}
			pr.response <- pb.Empty{}
		case decision := <-paxos.DecisionChan:
			// replica received a decision from a leader
			log.Printf("Replica %v received decision from leader %v with slot num %v and command %v", id, decision.arg.Id, decision.arg.SlotNum, decision.arg.Cmd.Cid)
			sn := decision.arg.SlotNum
			cd := decision.arg.Cmd
			replicaDecisions[sn] = *cd
			if sn >= minSlot {
				minSlot = sn + 1
			}
			for {
				cPrime, ok := replicaDecisions[slotNum]
				if !ok {
					break
				}
				cPP, ok1 := replicaProposals[slotNum]
				if ok1 && cPP.command.Cid != cPrime.Cid {
					sPrime := propose(cPP)
					if sPrime != -1 {
						for i, v := range leaders {
							if v {
								if i == id {
									go func(p string) {
										c := make(chan pb.Empty)
										paxos.ProposeChan <- ProposeInput{arg: &pb.SlotCmd{SlotNum: sPrime, Cmd: &cPP.command, Id: id}, response: c}
										result := <-c
										log.Printf("Return from propose %v", result)
									}(i)
								} else {
									go func(c pb.PaxosClient, p string) {
										ret, err := c.Propose(context.Background(), &pb.SlotCmd{SlotNum: sPrime, Cmd: &cPP.command, Id: id})
										if err != nil {
											log.Printf("Return from propose %v", ret)
										}
									}(peerClients[i], i)
								}
							}
						}
					}
				}
				perform(cPrime)
			}
			decision.response <- pb.Empty{}
		case p1b := <-prepareResponseChan:
			// proposer received a response to a previous prepare request
			if p1b.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", p1b.err)
			} else {
				log.Printf("Proposer %v got response to prepare request from %v", id, p1b.peer)
				log.Printf("Acceptor responded with ballot num %v, %v", p1b.ret.BallotNum, p1b.ret.BallotID)
				bn := p1b.ret.BallotNum
				bi := p1b.ret.BallotID
				if bn == myBallotNum && bi == id {
					for i, v := range p1b.ret.Accepted {
						log.Printf("Arg accepted index %v value %+v", i, *v)
						//sn := v.SlotNum
						if len(scoutPvals) > i {
							// check if v is empty
							if (scoutPvals[i]).BallotID == "" {
								scoutPvals[i] = v
							} else {
								if v.BallotNum > (scoutPvals[i]).BallotNum || (v.BallotNum == (scoutPvals[i]).BallotNum && v.BallotID > (scoutPvals[i]).BallotID) {
									scoutPvals[i] = v
								}
							}
						} else {
							scoutPvals = append(scoutPvals, v)
						}
					}
					scoutWaitFor--
					log.Printf("scout wait for: %v", scoutWaitFor)
					if float64(scoutWaitFor) < float64((len(*peers)+1))/float64(2) {
						// adopted: MORE LOGS possibly
						log.Printf("adopted")
						for _, v := range scoutPvals {
							if v.BallotID != "" {
								leaderProposals[v.SlotNum] = *v.Cmd
								log.Printf("leader proposals slot %v command cid %v", v.SlotNum, v.Cmd.Cid)
							}
						}
						commanderWaitFor = make(map[int64]int)
						for i, v := range leaderProposals {
							commanderWaitFor[i] = len(*peers) + 1
							// leader sends accept requests to acceptors
							for p, c := range peerClients {
								go func(c pb.PaxosClient, p string, slotArg int64, cArg pb.Command) {
									ret, err := c.Accept(context.Background(), &pb.P2A{LeaderID: id, Pv: &pb.Pvalue{BallotID: id, BallotNum: myBallotNum, SlotNum: slotArg, Cmd: &cArg}})
									acceptResponseChan <- AcceptResponse{ret: ret, err: err, peer: p}
								}(c, p, i, v)
							}
							// send to self
							go func(p string, slotArg int64, cArg pb.Command) {
								c := make(chan pb.P2B)
								paxos.AcceptChan <- AcceptInput{arg: &pb.P2A{LeaderID: p, Pv: &pb.Pvalue{BallotID: id, BallotNum: myBallotNum, SlotNum: slotArg, Cmd: &cArg}}, response: c}
								result := <-c
								acceptResponseChan <- AcceptResponse{ret: &result, err: nil, peer: p}
							}(id, i, v)
						}
						leaders[id] = true
						active = true
						scoutWaitFor += 2 * len(*peers)
					}
				} else {
					if bn > myBallotNum || (bn == myBallotNum && bi > id) {
						// preempted
						delete(leaders, id)
						active = false
						myBallotNum = bn
						restartTimer(timer, r, 50*int(myBallotNum)+1000, 50*int(myBallotNum)+4000)
					}
				}
			}
		case p2b := <-acceptResponseChan:
			// leader received a response to a previous accept RPC call from acceptor
			if p2b.err != nil {
				log.Printf("Error calling RPC %v", p2b.err)
			} else {
				log.Printf("Leader %v got response to accept request from %v", id, p2b.peer)
				bn := p2b.ret.BallotNum
				bi := p2b.ret.BallotID
				log.Printf("Acceptor returned ballot %v, %v with slot %v and command %v", bn, bi, p2b.ret.SlotNum, p2b.ret.Cmd.Cid)
				if bn == myBallotNum && bi == id {
					commanderWaitFor[p2b.ret.SlotNum]--
					log.Printf("commander wait for: %v", commanderWaitFor[p2b.ret.SlotNum])
					if float64(commanderWaitFor[p2b.ret.SlotNum]) < float64((len(*peers)+1))/float64(2.0) {
						// accepted
						// leader sends decisions to replicas
						for p, c := range peerClients {
							log.Printf("sending decison requests")
							go func(c pb.PaxosClient, p string) {
								ret, err := c.Decision(context.Background(), &pb.SlotCmd{SlotNum: p2b.ret.SlotNum, Cmd: p2b.ret.Cmd, Id: id})
								if err == nil {
									log.Printf("Return from decision %v", ret)
								} else {
									log.Printf("err: %v", err)
								}
							}(c, p)
						}
						// send to self
						go func(p string) {
							c := make(chan pb.Empty)
							paxos.DecisionChan <- DecisionInput{arg: &pb.SlotCmd{SlotNum: p2b.ret.SlotNum, Cmd: p2b.ret.Cmd, Id: id}, response: c}
							result := <-c
							log.Printf("Return from decision %v", result)
						}(id)
						commanderWaitFor[p2b.ret.SlotNum] += 2 * len(*peers)
					}
				} else {
					if bn > myBallotNum || (bn == myBallotNum && bi > id) {
						// preempted
						delete(leaders, id)
						active = false
						myBallotNum = bn
						restartTimer(timer, r, 50*int(myBallotNum)+1000, 50*int(myBallotNum)+4000)
					}
				}
			}

		}
	}
	log.Printf("Strange to arrive here")
}
