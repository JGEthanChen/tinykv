package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).

	// write use user key and commit timestamp as new key
	// and stores a write data structure
	putWrite := storage.Modify{Data: storage.Put{
		Key: EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf: engine_util.CfWrite,
	}}
	txn.writes = append(txn.writes, putWrite)

}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).

	val,err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	// val is empty, means no lock on key
	if val  == nil {
		return nil, nil
	}
	// val exists, parser that
	lock,err := ParseLock(val)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).

	//Lock structure directly use user key, Ts, TTL and Primary are stored as value
	putLock := storage.Modify{Data: storage.Put{
		Key: key,
		Value: lock.ToBytes(),
		Cf: engine_util.CfLock,
	}}
	txn.writes = append(txn.writes, putLock)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).

	//Lock structure directly use user key, Just need Cf and key
	deleteLock := storage.Modify{Data: storage.Delete{
		Key: key,
		Cf: engine_util.CfLock,
	}}
	txn.writes = append(txn.writes, deleteLock)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).

	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// Keys are encoded first by user key (ascending), then by timestamp (descending)
	// So as for a user key, the encoded keys are set as the order from recent to old
	// Seek func will find the first key that version older than StartTS (which is just behind the StartTS), if such a key exists
	iter.Seek(EncodeKey(key, txn.StartTS))

	// no such a key even if seek to the end
	if !iter.Valid() {
		return nil,nil
	}
	item := iter.Item()
	itemKey := item.Key()
	// no such a key, seek func find another user key in baking engine
	if !bytes.Equal(DecodeUserKey(itemKey), key) {
		return nil, nil
	}
	val,err := item.Value()
	if err != nil {
		return nil, err
	}

	write,err := ParseWrite(val)
	if err != nil {
		return nil, err
	}
	// get the final result by write
	result,err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))

	// guarantee if err occurs, return nil, err
	if err != nil {
		return nil, err
	}
	return result, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).

	// default CF use user key and the start timestamp
	// and stores the user value only
	putValue :=  storage.Modify{Data: storage.Put{
		Key: EncodeKey(key, txn.StartTS),
		Value: value,
		Cf: engine_util.CfDefault,
	}}
	txn.writes = append(txn.writes, putValue)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).

	// default CF use user key and the start timestamp
	// delete only needs key and cf
	deleteValue := storage.Modify{Data: storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf: engine_util.CfDefault,
	}}
	txn.writes = append(txn.writes, deleteValue)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).

	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// As for a certain user key, its encoded keys are set in the order from recent timestamp to old
	// Seek the first key which commit timestamp is smaller than TsMax, considered as the most recent write,
	// as long as write generate in this transaction
	iter.Seek(EncodeKey(key, TsMax))
	// From most recent to old timestamp
	for ;iter.Valid();iter.Next() {
		item := iter.Item()
		itemKey := item.Key()
		// no such a key, means all encoded keys with that user key are searched
		if !bytes.Equal(DecodeUserKey(itemKey), key) {
			break
		}
		commitTimeStamp := decodeTimestamp(itemKey)
		val,err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write,err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}
		// Oops, the current write is not with this transaction's start timestamp, try next
		if write.StartTS != txn.StartTS {
			continue
		}
		return write, commitTimeStamp, nil
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).


	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// As for a certain user key, its encoded keys are set in the order from recent timestamp to old
	// Seek the first key which commit timestamp is smaller than TsMax, considered as the most recent write,
	iter.Seek(EncodeKey(key, TsMax))
	if !iter.Valid() {
		return nil,0,nil
	}
	item := iter.Item()
	itemKey := item.Key()

	// no such a key
	if !bytes.Equal(DecodeUserKey(itemKey), key) {
		return nil,0,nil
	}
	commitTimeStamp := decodeTimestamp(itemKey)
	val,err := item.Value()
	if err != nil {
		return nil, 0, err
	}

	write,err := ParseWrite(val)
	if err != nil {
		return nil, 0, err
	}

	return write, commitTimeStamp, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
