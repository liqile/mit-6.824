package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	view View

	p_pinged map[string]time.Time
	b_pinged map[string]time.Time

	primary_acked uint
}

//
// server Ping RPC handler.
//

func (vs *ViewServer) IsAcked(is_primary_die bool) bool {
	//	if is_primary_die {
	//		return (vs.view.Viewnum == vs.primary_acked) || ((vs.view.Viewnum - 1) == vs.primary_acked)
	//	}
	return vs.view.Viewnum == vs.primary_acked
}

func (vs *ViewServer) PromoteBackup() {
	vs.view.Primary = vs.view.Backup
	vs.view.Backup = ""
	vs.view.Viewnum++
}
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	//first primary
	if args.Viewnum == 0 && vs.view.Primary == "" {
		vs.view.Primary = args.Me
		vs.view.Viewnum++
		vs.p_pinged[vs.view.Primary] = time.Now()
		vs.primary_acked = 0
	} else if args.Me == vs.view.Primary {
		//if primary failed and restart immediately
		if args.Viewnum == 0 {
			vs.PromoteBackup()
			vs.view.Backup = args.Me
		} else {
			//if primary failed and restart immediately
			vs.primary_acked = args.Viewnum
			vs.p_pinged[vs.view.Primary] = time.Now()
		}
	} else if vs.view.Backup == "" && vs.IsAcked(false) {
		//update the backup server
		vs.view.Backup = args.Me
		vs.view.Viewnum++
		vs.b_pinged[vs.view.Backup] = time.Now()
	} else if args.Me == vs.view.Backup {
		vs.b_pinged[vs.view.Backup] = time.Now()
	}
	reply.View = vs.view

	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	duration := time.Now().Sub(vs.p_pinged[vs.view.Primary])
	is_primary_die := duration > DeadPings*PingInterval
	if vs.view.Primary != "" && is_primary_die && vs.IsAcked(is_primary_die) {
		vs.PromoteBackup()
	}
	duration = time.Now().Sub(vs.b_pinged[vs.view.Backup])
	if vs.view.Backup != "" && duration > DeadPings*PingInterval && vs.IsAcked(false) {
		vs.view.Backup = ""
		vs.view.Viewnum++
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.p_pinged = make(map[string]time.Time)
	vs.b_pinged = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
