package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
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

	// (Used in 4B)
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
	var keys = [][]byte{req.Key}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if lock != nil && lock.Ts < req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil && ts >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				}})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mutation.Key),
			})
			continue
		}

		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
		txn.PutValue(mutation.Key, mutation.Value)
	}

	if len(resp.Errors) > 0 {
		return resp, nil
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			return resp, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if lock != nil && lock.Ts <= req.Version {
			pairs = append(pairs, &kvrpcpb.KvPair{Error: &kvrpcpb.KeyError{Locked: lock.Info(key)}, Key: key})
			continue
		}
		if key == nil {
			break
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: value,
		})

	}
	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.Action = kvrpcpb.Action_NoAction
		resp.CommitVersion = ts
		return resp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if lock == nil {
		if write == nil {
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			resp.Action = kvrpcpb.Action_LockNotExistRollback
		}
	} else if lock.Ts != req.LockTs {
		resp.Action = kvrpcpb.Action_NoAction
	} else if lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(lock.Ts) {
		// Lock timeout
		txn.DeleteValue(req.PrimaryKey)
		txn.DeleteLock(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		resp.LockTtl = lock.Ttl
		resp.Action = kvrpcpb.Action_NoAction
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "Transaction has committed"}
			}
			return resp, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock == nil {
			txn.DeleteValue(key)
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		if lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	klPairs, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	for _, klPair := range klPairs {
		if req.CommitVersion > 0 {
			txn.DeleteLock(klPair.Key)
			txn.PutWrite(klPair.Key, req.CommitVersion, &mvcc.Write{
				StartTS: klPair.Lock.Ts,
				Kind:    klPair.Lock.Kind,
			})
		} else {
			txn.DeleteLock(klPair.Key)
			txn.DeleteValue(klPair.Key)
			txn.PutWrite(klPair.Key, req.StartVersion, &mvcc.Write{
				StartTS: klPair.Lock.Ts,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
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
