package shardmaster

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
import "math"
import "reflect"
import "sort"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	cur_seq int
	encoder *gob.Encoder
	decoder *gob.Decoder
}

const (
	joinFlag = iota
	leaveFlag
	moveFlag
	queryFlag
)

type Op struct {
	// Your data here.
	Type    int
	Shard   int
	GID     int64
	Servers []string
}

// for scan the map in order
type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Int64Slice) Sort() {
	sort.Sort(s)
}

// generate a new configuration when join, leave or move
// copy the last configuration to a new one
func (sm *ShardMaster) generateNewConfig() {
	config_size := len(sm.configs) - 1
	var config Config
	config.Num = config_size + 1
	config.Shards = sm.configs[config_size].Shards
	config.Groups = make(map[int64][]string)

	for k, v := range sm.configs[config_size].Groups {
		config.Groups[k] = v
	}

	sm.configs = append(sm.configs, config)
}

// reconfigure the shard map when joinning a new group
// calculate the number of movement of previous groups
func (sm *ShardMaster) joinSchedule(gid int64) {

	config_last := len(sm.configs) - 1

	counts := map[int64]int{}
	for _, g := range sm.configs[config_last].Shards {
		counts[g] += 1
	}

	migration := make(map[int64]int)
	groups_size := len(sm.configs[config_last].Groups)
	last_size := NShards/groups_size + 1
	// the min migration size
	migration_size := NShards / (groups_size + 1)
	if migration_size == 0 {
		return
	}

	//make sure the traverse of map is sorted
	var sorted_keys Int64Slice
	for k, _ := range counts {
		sorted_keys = append(sorted_keys, k)
	}
	sorted_keys.Sort()

	// first move the bigger groups
	for _, k := range sorted_keys {
		if counts[k] == last_size {
			migration[k] = 1
			migration_size--
		} else {
			migration[k] = 0
		}
		if migration_size == 0 {
			break
		}
	}
	//then move the remain groups
	if migration_size > 0 {
		average_size := migration_size / groups_size
		remain_size := migration_size % groups_size

		var sec_sorted_keys Int64Slice
		for k, _ := range sm.configs[config_last].Groups {
			sec_sorted_keys = append(sec_sorted_keys, k)
		}
		sec_sorted_keys.Sort()

		for _, k := range sec_sorted_keys {
			if remain_size != 0 {
				migration[k] += average_size + 1
				remain_size--
			} else {
				migration[k] += average_size
			}
		}
	}

	//reconfigure the shard map
	for i, v := range sm.configs[config_last].Shards {
		if count, exist := migration[v]; exist && count > 0 {
			sm.configs[config_last].Shards[i] = gid
			migration[v]--
		}
	}
}

func (sm *ShardMaster) joinConfiguration(gid int64, servers []string) {
	sm.generateNewConfig()

	num := len(sm.configs) - 1
	if len(sm.configs[num].Groups) > 0 {
		_, exist := sm.configs[num].Groups[gid]
		if !exist {
			sm.joinSchedule(gid)
		}
	} else {
		for j, _ := range sm.configs[num].Shards {
			sm.configs[num].Shards[j] = gid
		}
	}

	sm.configs[num].Groups[gid] = servers
}

// reconfigure the shard map when a group leave
// first, find the groups with zero shard;
// if there isn't any one, calculate the number of shards each group should get.
func (sm *ShardMaster) leaveSchedule(gid int64) {
	config_last := len(sm.configs) - 1

	counts := map[int64]int{}
	for _, g := range sm.configs[config_last].Shards {
		counts[g] += 1
	}

	if _, exist := counts[gid]; !exist {
		return
	}
	groups_size := len(sm.configs[config_last].Groups)
	migration := make(map[int64]int)
	//check whether there is a group withtout shard
	var sorted_keys Int64Slice
	for k, _ := range sm.configs[config_last].Groups {
		sorted_keys = append(sorted_keys, k)
	}
	sorted_keys.Sort()

	for _, k := range sorted_keys {
		if _, exist := counts[k]; !exist {
			migration[k] = counts[gid]
		}
	}

	if len(migration) == 0 {
		migration_size := counts[gid] / groups_size
		remain_size := counts[gid] % groups_size
		if remain_size == 0 {
			for _, k := range sorted_keys {
				migration[k] = migration_size
			}
		} else {
			for _, k := range sorted_keys {
				if remain_size > 0 {
					migration[k] = migration_size + 1
					remain_size--
				} else {
					migration[k] = migration_size
				}
			}
		}
	}

	//reconfigure the shards map
	var counts_sorted_keys Int64Slice
	for k, _ := range migration {
		counts_sorted_keys = append(counts_sorted_keys, k)
	}
	counts_sorted_keys.Sort()
	index := 0
	for i, v := range sm.configs[config_last].Shards {
		if counts[gid] == 0 {
			break
		} else if v == gid {
			k := counts_sorted_keys[index]
			if migration[k] > 0 {
				sm.configs[config_last].Shards[i] = k
				migration[k]--
			}
			if migration[k] == 0 {
				index++
			}
			counts[gid] -= 1
		}
	}
}

func (sm *ShardMaster) leaveConfiguration(gid int64) {
	sm.generateNewConfig()

	num := len(sm.configs) - 1
	delete(sm.configs[num].Groups, gid)

	sm.leaveSchedule(gid)
}

func (sm *ShardMaster) moveSchedule(shard int, gid int64) {
	config_last := len(sm.configs) - 1

	old_gid := sm.configs[config_last].Shards[shard]
	sm.configs[config_last].Shards[shard] = gid

	min := math.MaxInt32
	max := 0
	counts := map[int64]int{}
	for _, g := range sm.configs[config_last].Shards {
		counts[g] += 1
	}

	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}

	if max-min > 1 {
		for i, g := range sm.configs[config_last].Shards {
			if g == gid {
				sm.configs[config_last].Shards[i] = old_gid
			}
		}
	}

}

//move configuration just modify the shard map
func (sm *ShardMaster) moveConfiguration(shard int, gid int64) {
	sm.generateNewConfig()
	num := len(sm.configs) - 1

	if _, exist := sm.configs[num].Groups[gid]; exist {
		//sm.moveSchedule(shard, gid)
		sm.configs[num].Shards[shard] = gid
	}
}

// The peer synchronize with others
func (sm *ShardMaster) Synchronize(max_seq int) {
	for sm.cur_seq <= max_seq {
		status, value := sm.px.Status(sm.cur_seq)
		//if peer fall behind at seq, restart an instance to catch up
		if status == paxos.Empty {
			sm.startInstance(sm.cur_seq, Op{})
			status, value = sm.px.Status(sm.cur_seq)
		}
		to := 10 * time.Millisecond
		for {
			if status == paxos.Decided {
				sm.encoder.Encode(value)
				var op Op
				sm.decoder.Decode(&op)
				if op.Type != queryFlag {
					switch op.Type {
					case joinFlag:
						sm.joinConfiguration(op.GID, op.Servers)
					case leaveFlag:
						sm.leaveConfiguration(op.GID)
					case moveFlag:
						sm.moveConfiguration(op.Shard, op.GID)
					}
				}
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, value = sm.px.Status(sm.cur_seq)
		}
		sm.cur_seq += 1
	}
	sm.px.Done(sm.cur_seq - 1)
}

func (sm *ShardMaster) startInstance(seq int, value Op) interface{} {
	var ans interface{}
	sm.px.Start(seq, value)
	to := 10 * time.Millisecond
	for {
		status, v := sm.px.Status(seq)
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

func (sm *ShardMaster) reachAgreement(value Op) {
	for {
		seq := sm.px.Max() + 1
		sm.Synchronize(seq - 1)
		v := sm.startInstance(seq, value)
		if reflect.DeepEqual(v.(Op), value) {
			sm.Synchronize(seq)
			break
		}
	}
}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	value := Op{
		Type:    joinFlag,
		Shard:   0,
		GID:     args.GID,
		Servers: args.Servers,
	}

	sm.reachAgreement(value)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	value := Op{
		Type:    leaveFlag,
		Shard:   0,
		GID:     args.GID,
		Servers: nil,
	}

	sm.reachAgreement(value)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	value := Op{
		Type:    moveFlag,
		Shard:   args.Shard,
		GID:     args.GID,
		Servers: nil,
	}

	sm.reachAgreement(value)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	value := Op{
		Type:    queryFlag,
		Shard:   0,
		GID:     0,
		Servers: nil,
	}
	sm.reachAgreement(value)

	config_index := args.Num
	if args.Num < 0 || args.Num >= len(sm.configs) {
		tmp := len(sm.configs)
		if tmp == 0 {
			config_index = 0
		} else {
			config_index = tmp - 1
		}
	}
	reply.Config.Num = config_index
	reply.Config.Shards = sm.configs[config_index].Shards
	reply.Config.Groups = make(map[int64][]string)

	for k, v := range sm.configs[config_index].Groups {
		reply.Config.Groups[k] = v
	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	//initialize

	var network bytes.Buffer
	sm.encoder = gob.NewEncoder(&network)
	sm.decoder = gob.NewDecoder(&network)
	sm.cur_seq = 0

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
