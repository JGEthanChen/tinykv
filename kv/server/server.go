package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
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
	reader,err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	val,err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error:err.Error()}, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	response := &kvrpcpb.RawGetResponse{
		Value: val,
		NotFound: false,
	}
	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	m := storage.Modify{
		Data: storage.Put{
			Key: req.Key,
			Value: req.Value,
			Cf: req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{m})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{
		RegionError:          nil,
		Error:                "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	del := storage.Modify{
		Data: storage.Delete{
			Cf: req.Cf,
			Key: req.Key,
		},
	}
	
	err := server.storage.Write(req.Context, []storage.Modify{del})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		},nil
	}
	return &kvrpcpb.RawDeleteResponse{
		RegionError:          nil,
		Error:                "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	response := &kvrpcpb.RawScanResponse{}
	reader,err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse {
			Error: err.Error(),
		},err
	}
	iter := reader.IterCF(req.Cf)
	defer reader.Close()
	defer iter.Close()

	iter.Seek(req.StartKey)
	for i:=uint32(0);iter.Valid() && i < req.Limit ; i++ {
		val,err := iter.Item().Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse {
				Error: err.Error(),
			},err
		}
		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
			Key: iter.Item().Key(),
			Value: val,
		})
		iter.Next()
	}
	return response, nil
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
		return resp, nil
	}

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

	// Get all keys for latches in one time
	var keys [][]byte

	for _,m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

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
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
