package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-aviral92/pb"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type ServerState struct {
	state     int
	voteCount int64
	leaderID  string

	currentTerm int64
	votedFor    string
	log         []*pb.Entry

	commitIndex int64
	lastApplied int64

	nextIndex  []int64
	matchIndex []int64
}

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 10000 //4000
	const DurationMin = 3000  //1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
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

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

func switchToCandidate(ss *ServerState, id string) {
	// Increment the term.
	ss.currentTerm++
	// Clear current Leader.
	ss.state = CANDIDATE
	ss.votedFor = id
	ss.voteCount = 1
	ss.leaderID = ""
}

func handleVoteRequest(ss *ServerState, arg *pb.RequestVoteArgs, timer *time.Timer, r *rand.Rand) (int64, bool, bool) {
	if arg.Term < ss.currentTerm {
		return ss.currentTerm, false, false
	}

	/*if len(ss.log)>0{
		if ss.log[len(ss.log)-1].Term > arg.LasLogTerm{
			return ss.currentTerm, false, false
		}else if ss.log[len(ss.log)-1].Term == arg.LasLogTerm{
			if int64(len(ss.log)) > arg.LastLogIndex{
				return ss.currentTerm, false, false
			}
		}
	}*/

	stepDown := false

	if arg.Term > ss.currentTerm {
		ss.currentTerm = arg.Term
		ss.votedFor = ""
		stepDown = true
	}

	//already leader
	if ss.state == LEADER && !stepDown {
		log.Printf("already leader")
		return ss.currentTerm, stepDown, false
	}

	//already casted vote in this term
	if ss.votedFor != "" && ss.votedFor != arg.CandidateID {
		log.Printf("Already caste vote in the term")
		return ss.currentTerm, stepDown, false
	}

	if len(ss.log) > 0 {
		if ss.log[len(ss.log)-1].Term > arg.LasLogTerm {
			log.Printf("step down here = %v and vote granted = false ", stepDown)
			return ss.currentTerm, stepDown, false
		} else if ss.log[len(ss.log)-1].Term == arg.LasLogTerm {
			if int64(len(ss.log)) > arg.LastLogIndex {
				log.Printf("step down = %v and vote granted = false ", stepDown)
				return ss.currentTerm, stepDown, false
			}
		}
	}

	//vote for candidate now
	ss.votedFor = arg.CandidateID

	restartTimer(timer, r)

	return arg.Term, stepDown, true
}

func wonElection(peerLen int64, votes int64) bool {
	if peerLen == 0 && votes >= 0 {
		return true
	} else if peerLen == 1 && votes >= 2 {
		return true
	} else if peerLen > 1 && votes >= 1+(peerLen+1)/2 {
		return true
	} else {
		log.Printf("Did not win election")
	}
	return false
}

func handleHeartbeat(ss *ServerState, arg *pb.AppendEntriesArgs, timer *time.Timer, r *rand.Rand) bool {

	//1
	if arg.Term < ss.currentTerm {
		return false
	}

	/*if ss.state == CANDIDATE && arg.Term >= ss.currentTerm {
		ss.currentTerm = arg.Term
		ss.votedFor = ""
		return true
	}*/

	if arg.Term >= ss.currentTerm {
		ss.currentTerm = arg.Term
		ss.votedFor = ""
		ss.state = FOLLOWER
		ss.voteCount = 0
		ss.leaderID = arg.LeaderID
		restartTimer(timer, r)
		//return true
	}

	//if len(arg.Entries) > 0{
	//log.Printf("we have got logs to append")

	//2 and 3
	if arg.PrevLogIndex == int64(len(ss.log)) && arg.PrevLogIndex != 0 {
		log.Printf("== case")
		if ss.log[arg.PrevLogIndex-1].Term != arg.PrevLogTerm {
			return false
		}
	} else if arg.PrevLogIndex > int64(len(ss.log)) {
		log.Printf(" > case")
		return false
	} else if arg.PrevLogIndex < int64(len(ss.log)) {
		log.Printf(" < case")
		if arg.PrevLogIndex != 0 {
			ss.log = ss.log[:arg.PrevLogIndex-1]
			if ss.log[arg.PrevLogIndex-1].Term != arg.PrevLogTerm {
				return false
			}
		} else {
			ss.log = ss.log[:0]
		}
	}

	//4
	for i := 0; i < len(arg.Entries); i++ {
		ss.log = append(ss.log, arg.Entries[i])
	}

	//5
	if arg.LeaderCommit > ss.commitIndex {
		if arg.LeaderCommit < int64(len(ss.log)) {
			ss.commitIndex = arg.LeaderCommit
		} else {
			ss.commitIndex = int64(len(ss.log))
		}
		log.Printf("commit Index of follower = %v ", ss.commitIndex)
	}

	for i := 0; i < len(ss.log); i++ {
		log.Printf("log state Index, Term, cmd = %v | %v | %v", ss.log[i].Index, ss.log[i].Term, ss.log[i].Cmd.Operation)
	}

	//}
	return true
}

func canCommit(ss *ServerState, peerLen int64) {
	for j := ss.commitIndex + 1; j <= ss.log[len(ss.log)-1].Index; j++ {
		voteCount := 1
		for i := range ss.matchIndex {
			index := ss.matchIndex[i]
			if index >= j && ss.log[j-1].Term == ss.currentTerm {
				voteCount++
			}
			if wonElection(peerLen, int64(voteCount)) {
				log.Printf("commitIndex = %v", ss.commitIndex)
				ss.commitIndex = j
			}
		}
	}
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)
	opHandler := make(map[int64]InputChannelType)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		err  error
		peer string
		//isHeartbeat bool
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	ss := &ServerState{
		state:     0,
		voteCount: 0,
		leaderID:  "",

		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.Entry, 0),

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int64, int64(len(*peers))+1),
		matchIndex: make([]int64, int64(len(*peers))+1),
	}

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))
	// Create a timer and start running it
	timerHB := time.NewTimer(time.Duration(2000) * time.Millisecond)

	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			if ss.state != LEADER {
				// The timer went off.
				log.Printf("Timeout")
				switchToCandidate(ss, id)
				//restartTimer(timer, r)
				log.Printf("Candidate = %v and Term = %v", id, ss.currentTerm)
				var lli, llt int64
				if len(ss.log) > 0 {
					lli = ss.log[len(ss.log)-1].Index
					llt = ss.log[len(ss.log)-1].Term
				} else {
					lli = 0
					llt = -1
				}
				log.Printf("lli = %v | llt = %v ", lli, llt)
				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string) {
						ret, err := c.RequestVote(context.Background(),
							&pb.RequestVoteArgs{
								Term:         ss.currentTerm,
								CandidateID:  id,
								LastLogIndex: lli,
								LasLogTerm:   llt})
						voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
					}(c, p)
				}
				// This will also take care of any pesky timeouts that happened while processing the operation.
				restartTimer(timer, r)
			}
		case op := <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command.
			//s.HandleCommand(op)
			if ss.state == LEADER {
				logToAppend := &pb.Entry{Term: ss.currentTerm,
					Index: int64(len(ss.log)) + 1,
					Cmd:   &op.command}
				ss.log = append(ss.log, logToAppend)
				opHandler[ss.log[len(ss.log)-1].Index] = op
				log.Printf("log appended")
			} else {
				log.Printf("Redirect here ")
				lID := ss.leaderID

				op.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: lID}}}

			}
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			log.Printf("Received append entry from %v", ae.arg.LeaderID)
			if handleHeartbeat(ss, ae.arg, timer, r) {
				ae.response <- pb.AppendEntriesRet{Term: ss.currentTerm, Success: true}
				if ss.commitIndex > ss.lastApplied {
					for i := ss.lastApplied + 1; i <= ss.commitIndex; i++ {
						//ss.lastApplied++
						//cmd := opHandler[ss.lastApplied]
						s.HandleCommandFollower(*ss.log[i-1].Cmd)
						log.Printf("last applied Index of follower = %v, commit index = %v ", ss.lastApplied, ss.commitIndex)
					}
					ss.lastApplied = ss.commitIndex
				}
			} else {
				ae.response <- pb.AppendEntriesRet{Term: ss.currentTerm, Success: false}
			}

			//ae.response <- pb.AppendEntriesRet{Term: ss.currentTerm, Success: true}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// TODO: Fix this.
			log.Printf("Received vote request from %v for term %v", vr.arg.CandidateID, vr.arg.Term)
			term, stepDown, voteGranted := handleVoteRequest(ss, vr.arg, timer, r)
			if stepDown {
				ss.state = FOLLOWER
				ss.voteCount = 0
				timerHB.Stop()
				restartTimer(timer, r)
			}
			vr.response <- pb.RequestVoteRet{Term: term, VoteGranted: voteGranted}
		case vr := <-voteResponseChan:
			// We received a response to a previous vote request.
			// TODO: Fix this
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
			} else {
				log.Printf("Got response to vote request from %v", vr.peer)
				log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)

				if vr.ret.Term > ss.currentTerm {
					log.Printf("Not Candidate anymore")
					ss.state = FOLLOWER
					ss.voteCount = 0
					ss.votedFor = ""
					ss.currentTerm = vr.ret.Term
					//ss.leaderID = ""
					restartTimer(timer, r)
				}

				if ss.state == CANDIDATE && vr.ret.VoteGranted && vr.ret.Term == ss.currentTerm {
					ss.voteCount++
					log.Printf("Peer len = %v and voteCount = %v", int64(len(*peers)), ss.voteCount)
					if wonElection(int64(len(*peers)), ss.voteCount) {
						ss.state = LEADER
						ss.leaderID = id
						for i := range ss.nextIndex {
							ss.nextIndex[i] = int64(len(ss.log)) + 1
						}
						for i := range ss.matchIndex {
							ss.matchIndex[i] = 0
						}

						log.Printf("%s is LEADER now. Send Heartbeat now.", id)
						for p, c := range peerClients {
							// Send in parallel so we don't wait for each client.
							var pli, plt int64
							var peerID = int64(p[4]) - 48

							pli = ss.nextIndex[peerID] - 1
							log.Printf("pli = %v", pli)
							if pli == 0 {
								plt = -1
							} else {
								plt = ss.log[pli-1].Term
							}

							logToSend := ss.log[len(ss.log):]

							go func(c pb.RaftClient, p string) {
								ret, err := c.AppendEntries(context.Background(),
									&pb.AppendEntriesArgs{
										Term:         ss.currentTerm,
										LeaderID:     id,
										PrevLogIndex: pli,
										PrevLogTerm:  plt,
										Entries:      logToSend})

								appendResponseChan <- AppendResponse{ret: ret,
									err:  err,
									peer: p}
								//isHeartbeat: true}
							}(c, p)
						}

						//This will also take care of any pesky timeouts that happened while processing the operation
						timerHB.Reset(time.Duration(2000) * time.Millisecond)
					}
				}
			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v while ar", ar.err)
			} else {

				log.Printf("Got append entries response from %v", ar.peer)
				var peerID = int64(ar.peer[4]) - 48
				if ar.ret.Term > ss.currentTerm {
					log.Printf("Not leader anymore")
					ss.state = FOLLOWER
					ss.voteCount = 0
					//ss.leaderID = id
					ss.votedFor = ""
					ss.currentTerm = ar.ret.Term
					restartTimer(timer, r)
					timerHB.Stop()
					break
				}

				//Success = False
				//if ss.state == LEADER && !ar.isHeartbeat {
				if !ar.ret.Success {
					log.Printf("decrement next index and try again for peer %v", ar.peer)
					ss.nextIndex[peerID]--
					if ss.nextIndex[peerID] <= 0 {
						ss.nextIndex[peerID] = 1
					}
				} else {
					log.Printf("Success - log appended to peer %v", ar.peer)
					ss.nextIndex[peerID]++
					if ss.nextIndex[peerID] > int64(len(ss.log)) {
						ss.nextIndex[peerID] = int64(len(ss.log)) + 1
					}
					ss.matchIndex[peerID] = ss.nextIndex[peerID] - 1
				}
				canCommit(ss, int64(len(*peers)))
				if ss.commitIndex > ss.lastApplied {
					for i := ss.lastApplied + 1; i <= ss.commitIndex; i++ {
						cmd, ok := opHandler[i]
						if !ok {
							log.Printf("not in map")
							s.HandleCommandFollower(*ss.log[i-1].Cmd)
						} else {
							log.Printf("found in map")
							s.HandleCommand(cmd)
						}
						log.Printf("last applied Index of leader = %v, commit index = %v ", i, ss.commitIndex)
					}
					ss.lastApplied = ss.commitIndex
				}

				//}
			}
		case <-timerHB.C:
			if ss.state == LEADER {
				log.Printf("Heartbeat Timeout")

				for p, c := range peerClients {
					var peerID = int64(p[4]) - 48
					log.Printf("Peer id is %v, next index is %v", peerID, ss.nextIndex[peerID])

					if len(ss.log) > 0 && ss.log[len(ss.log)-1].Index >= ss.nextIndex[peerID] {
						log.Printf("sending logs with heartbeat")
						var pli, plt int64

						pli = ss.nextIndex[peerID] - 1
						log.Printf("pli = %v", pli)
						if pli == 0 {
							plt = -1
						} else {
							plt = ss.log[pli-1].Term
						}
						logToSend := ss.log[pli : pli+1]
						for i := 0; i < len(logToSend); i++ {
							log.Printf("log to send Index = %v | Term = %v | cmd = %v ", logToSend[i].Index, logToSend[i].Term, logToSend[i].Cmd.Operation)
						}

						// Send in parallel so we don't wait for each client.
						go func(c pb.RaftClient, p string) {
							ret, err := c.AppendEntries(context.Background(),
								&pb.AppendEntriesArgs{
									Term:         ss.currentTerm,
									LeaderID:     id,
									PrevLogIndex: pli,
									PrevLogTerm:  plt,
									Entries:      logToSend,
									LeaderCommit: ss.commitIndex})
							appendResponseChan <- AppendResponse{ret: ret,
								err:  err,
								peer: p}
						}(c, p)

					} else {
						log.Printf("Sending heartbeat")
						// Send heartbeat in parallel so we don't wait for each client.
						var pli, plt int64

						pli = ss.nextIndex[peerID] - 1
						log.Printf("Heartbeat pli = %v", pli)
						if pli == 0 {
							plt = -1
						} else {
							plt = ss.log[pli-1].Term
						}

						logToSend := ss.log[len(ss.log):]

						go func(c pb.RaftClient, p string) {
							ret, err := c.AppendEntries(context.Background(),
								&pb.AppendEntriesArgs{
									Term:         ss.currentTerm,
									LeaderID:     id,
									PrevLogIndex: pli,
									PrevLogTerm:  plt,
									Entries:      logToSend,
									LeaderCommit: ss.commitIndex})

							appendResponseChan <- AppendResponse{ret: ret,
								err:  err,
								peer: p}
							//isHeartbeat : true}
						}(c, p)
					}
				}
				//This will also take care of any pesky timeouts that happened while processing the operation
				timerHB.Reset(time.Duration(2000) * time.Millisecond)
			}
		}
	}
	log.Printf("Strange to arrive here")
}
