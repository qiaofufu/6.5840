package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data   map[string]string
	record map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[args.Key]
	if ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
	if seq, ok := kv.record[args.ClientID]; ok && seq >= args.Sequence {
		reply.Value = ""
		return
	}
	kv.record[args.ClientID] = args.Sequence
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if seq, ok := kv.record[args.ClientID]; !ok || seq+1 == args.Sequence {
		kv.data[args.Key] = args.Value
	}
	kv.record[args.ClientID] = args.Sequence
	kv.mu.Unlock()
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seq, ok := kv.record[args.ClientID]; !ok || seq+1 == args.Sequence {
		old, ok := kv.data[args.Key]
		if ok {
			kv.data[args.Key] = old + args.Value
		} else {
			kv.data[args.Key] = args.Value
		}
		reply.Value = old
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.record = make(map[int64]int64)
	return kv
}
