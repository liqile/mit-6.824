package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

//------------------COMMETS----------------
//1. Procedure
//	 Phase 1 : prepare. select a majority of servers, send prepare request
//	 Phase 2 : accept. when a majority of servers promised, send accept request
//	 Phase 3 : decided. when a majority of servers accepted, send decided request to all
//2. The commit point of one aggreement is all peers decided,
//	 mainly because of unreliable net.
//3. Progress : sleep random time when any phase failed to prevent the progress
import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "math"
import "time"
import "strconv"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const (
	InitialValue = -1
)

type InstanceState struct {
	//state for promise and accept
	promised_n string
	accepted_n string
	accepted_v interface{}
	status     Fate
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	//majority size
	majority_size int
	//instance status
	instances map[int]*InstanceState
	//min max
	done_seq int
	max_seq  int
	min_seq  int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

const (
	RejectSignal   = -1
	PromisedSignal = 1
)

type PrepareArgs struct {
	Pid         int
	ProposalNum string
}

type PrepareReply struct {
	Promised  int
	AcceptedN string
	AcceptedV interface{}
}

type AcceptArgs struct {
	Pid         int
	ProposalNum string
	Value       interface{}
}

type AcceptReply struct {
	Accepted bool
}

type DecidedArgs struct {
	Pid         int
	ProposalNum string
	Value       interface{}
}

type DecidedReply struct {
	Done int
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Promised = RejectSignal
	is_promise := false
	if _, exist := px.instances[args.Pid]; exist {
		if px.instances[args.Pid].promised_n < args.ProposalNum {
			is_promise = true
		}
	} else {
		px.instances[args.Pid] = &InstanceState{
			promised_n: "",
			accepted_n: "",
			accepted_v: nil,
			status:     0,
		}

		is_promise = true
	}
	if is_promise {
		px.instances[args.Pid].promised_n = args.ProposalNum
		reply.Promised = PromisedSignal
		reply.AcceptedN = px.instances[args.Pid].accepted_n
		reply.AcceptedV = px.instances[args.Pid].accepted_v
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, exist := px.instances[args.Pid]
	if !exist {
		px.instances[args.Pid] = &InstanceState{
			promised_n: "",
			accepted_n: "",
			accepted_v: nil,
			status:     0,
		}
	}
	if args.ProposalNum >= px.instances[args.Pid].promised_n {
		px.instances[args.Pid].promised_n = args.ProposalNum
		px.instances[args.Pid].accepted_n = args.ProposalNum
		px.instances[args.Pid].accepted_v = args.Value
		px.instances[args.Pid].status = Pending
		reply.Accepted = true
	} else {
		reply.Accepted = false
	}
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, exist := px.instances[args.Pid]
	if exist {
		px.instances[args.Pid].accepted_n = args.ProposalNum
		px.instances[args.Pid].accepted_v = args.Value
		px.instances[args.Pid].status = Decided
	} else {
		px.instances[args.Pid] = &InstanceState{
			"",
			args.ProposalNum,
			args.Value,
			Decided,
		}
	}
	if args.Pid > px.max_seq {
		px.max_seq = args.Pid
	}
	reply.Done = px.done_seq
	return nil
}

func generateIncreasingNum(me int) string {
	return strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.Itoa(me)
}

func (px *Paxos) selectMajority() []string {
	majority := make(map[int]bool)
	answer := make([]string, 0, px.majority_size)
	size := len(px.peers)
	i := 0
	for i < px.majority_size {
		rand_num := rand.Intn(size)
		if _, exist := majority[rand_num]; exist {
			continue
		} else {
			answer = append(answer, px.peers[rand_num])
			majority[rand_num] = true
			i++
		}
	}
	return answer
}

func (px *Paxos) sendPrepare(acceptors []string, seq int, proposal_num string, v interface{}) (int, interface{}) {
	prepared_servers := 0
	args := &PrepareArgs{seq, proposal_num}
	max_value := v
	max_seq := ""
	for i, server := range acceptors {
		var reply PrepareReply
		ret := false
		if i != px.me {
			ret = call(server, "Paxos.Prepare", args, &reply)
		} else {
			ret = px.Prepare(args, &reply) == nil
		}
		if ret && reply.Promised == PromisedSignal {
			if reply.AcceptedN > max_seq {
				max_seq = reply.AcceptedN
				max_value = reply.AcceptedV
			}
			prepared_servers += 1
		}
	}
	return prepared_servers, max_value
}

func (px *Paxos) sendAccept(acceptors []string, pid int, proposal_num string, max_value interface{}) int {
	accepted_servers := 0
	accept_args := &AcceptArgs{pid, proposal_num, max_value}
	for i, server := range acceptors {
		var accept_reply AcceptReply
		ret := false
		if i != px.me {
			ret = call(server, "Paxos.Accept", accept_args, &accept_reply)
		} else {
			ret = px.Accept(accept_args, &accept_reply) == nil
		}
		if ret && accept_reply.Accepted {
			accepted_servers += 1
		}
	}
	return accepted_servers
}

func (px *Paxos) sendDecided(pid int, proposal_num string, max_value interface{}) {
	decide_args := &DecidedArgs{pid, proposal_num, max_value}
	all_decided := false
	min_done := math.MaxInt32
	for !all_decided {
		all_decided = true
		for i, server := range px.peers {
			var reply DecidedReply
			ret := false
			if i != px.me {
				ret = call(server, "Paxos.Decided", decide_args, &reply)
			} else {
				ret = px.Decided(decide_args, &reply) == nil
			}
			if !ret {
				all_decided = false
			} else if reply.Done < min_done {
				min_done = reply.Done
			}
		}
		if !all_decided {
			time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
		}
	}
	if min_done != InitialValue {
		px.mu.Lock()
		if min_done > px.done_seq {
			for key, _ := range px.instances {
				if key <= min_done {
					delete(px.instances, key)
				}
			}
			px.done_seq = min_done
		}
		px.min_seq = min_done + 1
		px.mu.Unlock()
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		if seq < px.min_seq {
			return
		}
		for true {
			//phase prepare
			proposal_num := generateIncreasingNum(px.me)
			acceptors := px.selectMajority()
			prepared_servers, max_value := px.sendPrepare(acceptors, seq, proposal_num, v)
			//phase accept
			if prepared_servers == px.majority_size {
				accepted_servers := px.sendAccept(acceptors, seq, proposal_num, max_value)
				//phase decided
				if accepted_servers == px.majority_size {
					px.sendDecided(seq, proposal_num, max_value)
					break
				} else {
					time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
			}
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.done_seq {
		for key, _ := range px.instances {
			if key <= seq {
				delete(px.instances, key)
			}
		}
		px.done_seq = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.max_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.min_seq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < px.min_seq {
		return Forgotten, nil
	}
	if instance, ok := px.instances[seq]; ok {
		return instance.status, instance.accepted_v
	} else {
		return Forgotten, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.majority_size = len(peers)/2 + 1
	px.instances = make(map[int]*InstanceState)
	px.done_seq = InitialValue
	px.min_seq = 0
	px.max_seq = InitialValue

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
