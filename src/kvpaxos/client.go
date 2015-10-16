package kvpaxos

import "net/rpc"
import "crypto/rand"
import "crypto/md5"
import "math/big"
import "io"
import "strconv"
import "time"

import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	me string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func smd5(text string) string {
	hashMd5 := md5.New()
	io.WriteString(hashMd5, text)
	return fmt.Sprintf("%x", hashMd5.Sum(nil))
}

func srand() string {
	nano := time.Now().UnixNano()
	rand_num := nrand()
	uuid := smd5(smd5(strconv.FormatInt(nano, 10)) + smd5(strconv.FormatInt(rand_num, 10)))

	return uuid
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = srand()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	uuid := strconv.FormatInt(nrand(), 10) + ck.me
	args := &GetArgs{
		key,
		uuid,
	}
	i := 0
	size := len(ck.servers)
	for {
		var reply GetReply
		ret := call(ck.servers[i], "KVPaxos.Get", args, &reply)
		if ret {
			return reply.Value
		}
		i = (i + 1) % size
	}
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	uuid := strconv.FormatInt(nrand(), 10) + ck.me
	args := &PutAppendArgs{
		key,
		value,
		op,
		uuid,
	}
	i := 0
	size := len(ck.servers)
	for {
		var reply PutAppendReply
		ret := call(ck.servers[i], "KVPaxos.PutAppend", args, &reply)
		if ret {
			break
		}
		i = (i + 1) % size
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
