package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "bytes"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Uid   string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	data           map[string]string
	request_number map[string]int
	cur_seq        int
	encoder        *gob.Encoder
	decoder        *gob.Decoder
}

// The peer synchronize with others
func (kv *KVPaxos) Synchronize(max_seq int, uid string) {
	for kv.cur_seq <= max_seq {
		status, value := kv.px.Status(kv.cur_seq)
		//if peer fall behind at seq, restart a instance to catch up
		if status == paxos.Empty {
			kv.startInstance(kv.cur_seq, Op{})
			status, value = kv.px.Status(kv.cur_seq)
		}
		to := 10 * time.Millisecond
		for {
			if status == paxos.Decided {
				kv.encoder.Encode(value)
				var op Op
				kv.decoder.Decode(&op)
				if op.Type != GetFlag {
					if _, duplicate := kv.request_number[op.Uid]; !duplicate {
						//fmt.Println("Decided", kv.me, op.Type, op.Key, op.Value)
						switch op.Type {
						case PutFlag:
							kv.data[op.Key] = op.Value
						case AppendFlag:
							_, exist := kv.data[op.Key]
							if exist {
								kv.data[op.Key] += op.Value
							} else {
								kv.data[op.Key] = op.Value
							}
						}
						kv.request_number[op.Uid] = 1
					}
				}
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, value = kv.px.Status(kv.cur_seq)
		}
		kv.cur_seq += 1
	}
	kv.px.Done(kv.cur_seq - 1)
}

// start a new instance
func (kv *KVPaxos) startInstance(seq int, value Op) interface{} {
	var ans interface{}
	kv.px.Start(seq, value)
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			ans = v
			break
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return ans
}

// Reach a aggreement until there is a log with the value
func (kv *KVPaxos) reachAgreement(value Op) {
	for {
		seq := kv.px.Max() + 1
		kv.Synchronize(seq-1, value.Uid)
		v := kv.startInstance(seq, value)
		if v == value {
			kv.Synchronize(seq, value.Uid)
			break
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.request_number[args.Uid]; !exist {
		value := Op{
			GetFlag,
			args.Key,
			"",
			args.Uid,
		}
		kv.reachAgreement(value)
	}
	if _, e := kv.data[args.Key]; e {
		reply.Err = OK
		reply.Value = kv.data[args.Key]
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.request_number[args.Uid]; !exist {
		value := Op{
			args.Op,
			args.Key,
			args.Value,
			args.Uid,
		}
		kv.reachAgreement(value)
	}
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.request_number = make(map[string]int)
	var network bytes.Buffer
	kv.encoder = gob.NewEncoder(&network)
	kv.decoder = gob.NewDecoder(&network)
	kv.cur_seq = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
