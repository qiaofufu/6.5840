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
	record map[int64]string
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
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.record[args.ClientID+args.Sequence]; ok {
		reply.Value = v
		return
	}
	kv.data[args.Key] = args.Value
	reply.Value = args.Value
	kv.record[args.ClientID+args.Sequence] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.record[args.ClientID+args.Sequence]; ok {
		reply.Value = v
		return
	}
	old, ok := kv.data[args.Key]
	if ok {
		kv.data[args.Key] = old + args.Value
	} else {
		kv.data[args.Key] = args.Value
	}
	reply.Value = old
	kv.record[args.ClientID+args.Sequence] = old
}

func (kv *KVServer) Complete(args *CompleteArgs, reply *CompleteReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.record[args.Key]; ok {
		delete(kv.record, args.Key)
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.record = make(map[int64]string)
	return kv
}
