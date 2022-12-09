package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pkg/errors"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn         *MvccTxn
	iter        engine_util.DBIterator
	done        bool
	closed      bool
	nextUserKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{txn: txn, iter: txn.Reader.IterCF(engine_util.CfWrite), nextUserKey: startKey}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.closed = true
	if scan.iter != nil {
		scan.iter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.closed {
		return nil, nil, errors.New("Scanner already closed")
	}
	if scan.done {
		return nil, nil, nil
	}

	var key, val []byte

	iter := scan.iter
	if iter.Seek(EncodeKey(scan.nextUserKey, scan.txn.StartTS)); !iter.Valid() {
		scan.done = true
		return nil, nil, nil
	}
	item := iter.Item()
	userKey := DecodeUserKey(item.Key())
	if !bytes.Equal(userKey, scan.nextUserKey) {
		scan.nextUserKey = userKey
		return scan.Next()
	}

	for {
		value, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, nil, err
		}
		if write == nil || write.Kind == WriteKindRollback {
			iter.Next()
			if !iter.Valid() {
				scan.done = true
				return nil, nil, nil
			}
			item = iter.Item()
			userKey = DecodeUserKey(item.Key())
			if !bytes.Equal(userKey, scan.nextUserKey) {
				scan.nextUserKey = userKey
				return scan.Next()
			}
			continue
		}
		if write.Kind == WriteKindPut {
			val, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			key = userKey
			for {
				iter.Next()
				if !iter.Valid() {
					scan.done = true
					return key, val, nil
				}
				item = iter.Item()
				userKey = DecodeUserKey(item.Key())
				if !bytes.Equal(userKey, scan.nextUserKey) {
					scan.nextUserKey = userKey
					return key, val, nil
				}
			}
		}

		// Key has been deleted
		for {
			iter.Next()
			if !iter.Valid() {
				scan.done = true
				return nil, nil, nil
			}
			item = iter.Item()
			userKey = DecodeUserKey(item.Key())
			if !bytes.Equal(userKey, scan.nextUserKey) {
				scan.nextUserKey = userKey
				return scan.Next()
			}
		}
	}
}
