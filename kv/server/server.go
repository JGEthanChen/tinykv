package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}

	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			resp.Error = resp.RegionError.Message
			return resp, err
		}
		return resp, nil
	}
	val,err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			resp.Error = resp.RegionError.Message
			return resp, err
		}
		return resp, nil
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawPutResponse{}


	err := server.storage.Write(req.Context, []storage.Modify{storage.Modify{
		Data: storage.Put {
			Cf: req.Cf,
			Key: req.Key,
			Value: req.Value,
		},
	}})
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		if len(resp.RegionError.Message) != 0 {
			resp.Error = resp.RegionError.Message
			return resp, err
		}
	}
	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawDeleteResponse{}
	
	err := server.storage.Write(req.Context, []storage.Modify{storage.Modify{
		Data: storage.Delete{
			Cf: req.Cf,
			Key: req.Key,
		},
	}})
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		if len(resp.RegionError.Message) != 0 {
			resp.Error = resp.RegionError.Message
			return resp, err
		}
	}
	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawScanResponse{}

	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		if len(resp.RegionError.Message) != 0 {
			resp.Error = resp.RegionError.Message
			return resp, err
		}
		return resp, nil
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)
	for count:=uint32(0);iter.Valid() && count < req.Limit; iter.Next() {
		item := iter.Item()
		val,err := item.Value()
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			if len(resp.RegionError.Message) != 0 {
				resp.Error = resp.RegionError.Message
				return resp, err
			}
			return resp, nil
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key: item.Key(),
			Value: val,
		})
		count++
	}
	return resp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}

	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			return nil, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	lock,err := mvccTxn.GetLock(req.Key)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		if len(resp.RegionError.Message) != 0 {
			return nil, err
		}
		return resp, nil
	}
	// check if the lock exists, and is locked by other transaction
	if lock != nil && lock.IsLockedFor(req.Key, mvccTxn.StartTS, resp) {
		resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
	} else {
		// get the value
		value,err := mvccTxn.GetValue(req.Key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			if len(resp.RegionError.Message) != 0 {
				return nil, err
			}
			return resp, nil
		}

		// value not found
		if value == nil {
			resp.NotFound = true
		} else {
			resp.Value = value
		}
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}

	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			return nil, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)


	server.waitForLatchesByMutations(req.Mutations)
	defer server.ReleaseLatchesByMutations(req.Mutations)

	// Loop check the lock and write
	for _,m := range req.Mutations {
		// check if have write conflict
		w,commitTImeStamp,err := mvccTxn.MostRecentWrite(m.Key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			if len(resp.RegionError.Message) != 0 {
				return nil, err
			}
			return resp, nil
		}
		// write conflicts
		if w != nil && commitTImeStamp >= mvccTxn.StartTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
						StartTs: mvccTxn.StartTS,
						ConflictTs: commitTImeStamp,
						Key: m.Key,
						Primary: req.PrimaryLock,
				},
			})
			continue
		}

		// check if the key is locked
		lock,err := mvccTxn.GetLock(m.Key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			if len(resp.RegionError.Message) != 0 {
				return nil, err
			}
			return resp, nil
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(m.Key)})
			continue
		}

		mvccTxn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts: mvccTxn.StartTS,
			Ttl: req.LockTtl,
			Kind: mvcc.WriteKindFromProto(m.Op),
		})

		// lock the key, and write the value
		switch m.Op {
		case kvrpcpb.Op_Put:
			mvccTxn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			mvccTxn.DeleteValue(m.Key)
		}
	}

	// no key error, the write to storage
	if len(resp.Errors) == 0 {
		err = server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			if len(resp.RegionError.Message) != 0 {
				return nil, err
			}
			return resp, nil
		}
	}
	return resp, nil
}


func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).

	resp := &kvrpcpb.CommitResponse{}

	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
			return resp, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _,key := range req.Keys {
		lock,err := mvccTxn.GetLock(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			if len(resp.RegionError.Message) != 0 {
				resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
				return resp, err
			}
			return resp, nil
		}
		// lock not found or keys are locked by other transactions
		if lock == nil {
			// situation that rolling back should be processed silently
			//resp.Error = &kvrpcpb.KeyError{Retryable: "Lock not found."}
			return resp, nil
		} else if lock.Ts != mvccTxn.StartTS {
			resp.Error = &kvrpcpb.KeyError{Retryable: "Keys are locked by other transaction."}
			return resp, nil
		}
		// generate the write, and write into storage
		mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			Kind: lock.Kind,
			StartTS: req.StartVersion,
		})
		mvccTxn.DeleteLock(key)
	}

	// write into storage if no error occurred
	if err := server.storage.Write(req.Context, mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		if len(resp.RegionError.Message) != 0 {
			resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
			return resp, err
		}
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}

	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			return resp, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	it := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer it.Close()
	for it.NextByIter(); it.Valid() && it.Count() <= req.Limit; it.NextByIter() {
		item := it.Item()
		// if have region err or other err, return resp directly
		if item.RegionError() != nil {
			resp.RegionError = item.RegionError()
			return resp, nil
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Error: &kvrpcpb.KeyError{
				Locked: item.KeyError().Locked,
			},
			Key: mvcc.DecodeUserKey(item.Key()),
			Value: item.Value(),
		})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			return resp, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock,err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			return resp, err
		}
		return resp, nil
	}

	if lock != nil && lock.Ts == mvccTxn.StartTS {
		if mvcc.PhysicalTime(lock.Ts) + lock.Ttl >= mvcc.PhysicalTime(req.CurrentTs) {
			// lock is not exhausted, return ttl info
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
			return resp, nil
		} else {
			// roll back the primary key
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			if lock.Kind == mvcc.WriteKindPut {
				mvccTxn.DeleteValue(req.PrimaryKey)
			}
			mvccTxn.DeleteLock(req.PrimaryKey)
			mvccTxn.PutWrite(req.PrimaryKey, mvccTxn.StartTS, &mvcc.Write{
				Kind: mvcc.WriteKindRollback,
				StartTS: mvccTxn.StartTS,
			})
			err := server.storage.Write(req.Context, mvccTxn.Writes())
			if err != nil {
				// change error into region error
				resp.RegionError = util.RaftstoreErrToPbError(err)
				// the error is not a region error, return error string
				if len(resp.RegionError.Message) != 0 {
					return resp, err
				}
			}
			return resp, nil
		}

	}

	//lock is nil, the transaction has previously been rolled back or committed
	resp.Action = kvrpcpb.Action_NoAction
	w, cts, err := mvccTxn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			return resp, err
		}
		return resp, nil
	}
	// if no write, then generate rollback write
	if w == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		mvccTxn.PutWrite(req.PrimaryKey, mvccTxn.StartTS, &mvcc.Write{
			Kind: mvcc.WriteKindRollback,
			StartTS: mvccTxn.StartTS,
		})
		err := server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {
			// change error into region error
			resp.RegionError = util.RaftstoreErrToPbError(err)
			// the error is not a region error, return error string
			if len(resp.RegionError.Message) != 0 {
				return resp, err
			}
		}
	}
	// if transaction has already rolled back, commitversion is empty
	if w != nil && w.Kind != mvcc.WriteKindRollback{
		resp.CommitVersion = cts
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).

	resp := &kvrpcpb.BatchRollbackResponse{}
	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
			return resp, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _,key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			// change error into region error
			resp.RegionError = util.RaftstoreErrToPbError(err)
			// the error is not a region error, return error string
			if len(resp.RegionError.Message) != 0 {
				resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
				return resp, err
			}
			return resp, nil
		}

		// locked by other transaction
		if lock != nil && lock.Ts != mvccTxn.StartTS {
			mvccTxn.PutWrite(key, mvccTxn.StartTS, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind: mvcc.WriteKindRollback,
			})
		}

		if lock == nil {
			w, _, err := mvccTxn.CurrentWrite(key)
			if err != nil {
				resp.RegionError = util.RaftstoreErrToPbError(err)
				if len(resp.RegionError.Message) != 0 {
					resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
					return resp, err
				}
				return resp, nil
			}

			if w == nil {
				// not commit successfully
				mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
			} else if w != nil  && w.Kind != mvcc.WriteKindRollback{
				// case that write should rollback, but not end in normal situation.
				resp.Error = &kvrpcpb.KeyError{Abort: "Can't roll back a transaction that has already committed."}
				return resp, nil
			}
			// other case that w != nil, means the transaction has already rollback
		}

		// roll back the value
		if lock != nil && lock.Ts == mvccTxn.StartTS {
			if lock.Kind == mvcc.WriteKindPut {
				mvccTxn.DeleteValue(key)
			}
			mvccTxn.DeleteLock(key)
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind: mvcc.WriteKindRollback,
			})
		}
	}

	if err := server.storage.Write(req.Context, mvccTxn.Writes()); err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		if len(resp.RegionError.Message) != 0 {
			resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
			return resp, err
		}
		return resp, nil
	}

	return resp, nil
}

func (server *Server) KvResolveLock(context context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	// Get the reader
	reader,err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
			return resp, err
		}
		return resp, nil
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	keys, err := mvcc.AllKeysInLocksForTxn(mvccTxn)
	if err != nil {
		// change error into region error
		resp.RegionError = util.RaftstoreErrToPbError(err)
		// the error is not a region error, return error string
		if len(resp.RegionError.Message) != 0 {
			resp.Error = &kvrpcpb.KeyError{Abort: "A fatal error occurred."}
			return resp, err
		}
		return resp, nil
	}

	if req.CommitVersion == 0 {
		// roll back all the keys
		rbResp,err := server.KvBatchRollback(context, &kvrpcpb.BatchRollbackRequest{
			Context: req.Context,
			StartVersion: req.StartVersion,
			Keys: keys,
		})
		if err != nil {
			return resp, err
		}
		resp.RegionError = rbResp.RegionError
		resp.Error = rbResp.Error
	} else {
		// commit keys
		commitResp,err := server.KvCommit(context, &kvrpcpb.CommitRequest{
			Context: req.Context,
			StartVersion: req.StartVersion,
			CommitVersion: req.CommitVersion,
			Keys: keys,
		})
		if err != nil {
			return resp, err
		}
		resp.RegionError = commitResp.RegionError
		resp.Error = commitResp.Error
	}

	return resp, nil
}

func (server *Server) waitForLatchesByMutations(mutations []*kvrpcpb.Mutation) {
	// Get all keys for latches in one time
	var keys [][]byte

	for _,m := range mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.WaitForLatches(keys)
}

func (server *Server) ReleaseLatchesByMutations(mutations []*kvrpcpb.Mutation) {
	// Get all keys for latches in one time
	var keys [][]byte

	for _,m := range mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.ReleaseLatches(keys)
}


// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
