package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	defer func() {
		if reader != nil {
			reader.Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &kvrpcpb.RawGetResponse{NotFound: true}, nil
		}
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	data := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	var batch []storage.Modify
	batch = append(batch, storage.Modify{Data: data})
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	data := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	var batch []storage.Modify
	batch = append(batch, storage.Modify{Data: data})
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	defer func() {
		if reader != nil {
			reader.Close()
		}
	}()
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	var kvPairs []*kvrpcpb.KvPair
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvPair := kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		}
		kvPairs = append(kvPairs, &kvPair)
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
