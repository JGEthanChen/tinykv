package mvcc

import (
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// means the last encoded key in the storage as for a certain user key.
const TsLast uint64 = uint64(0)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter engine_util.DBIterator
	count uint32
	txn *MvccTxn
	item *ScannerItem
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	scan := &Scanner{
		iter: it,
		txn: txn,
		item: &ScannerItem{},
	}
	scan.iter.Seek(EncodeKey(startKey, scan.txn.StartTS))
	// get the first key as a start
	scan.item.key = scan.iter.Item().Key()
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Valid judge whether scanner should go next
func (scan *Scanner) Valid() bool {
	// Your Code Here (4C).
	if scan.item.key != nil {
		return true
	}
	return false
}

func (scan *Scanner) fresh() {
	scan.item = &ScannerItem{}
}
// Item return the item in scanner
func (scan *Scanner) Item() ScannerItem {
	return *scan.item
}

// Count return read counts
func (scan *Scanner) Count() uint32 {
	return scan.count
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
// Just for test
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	scan.NextByIter()
	return scan.item.key, scan.item.value, scan.item.err
}

// **Modified** Overloading Next.
// In order to simplify the scanner, Next here just drive iter next, and record current key, value, error or lock error in ScannerItem
// User can get the item info by scanner, and check when stop by Valid.
func (scan *Scanner) NextByIter() {
	// Your Code Here (4C).

	// Avoid using Next without checking valid, hardly appear
	// In this case, make sure the item empty
	if !scan.iter.Valid() {
		scan.fresh()
		return
	}

	curKey := DecodeUserKey(scan.iter.Item().Key())
	cts := decodeTimestamp(scan.iter.Item().Key())
	scan.fresh()

	// If the write's commit timestamp > transaction.ts, seek iter directly to the front of transaction.ts
	if cts >= scan.txn.StartTS {
		scan.iter.Seek(EncodeKey(curKey, scan.txn.StartTS))
		scan.NextByIter()
		return
	}

	wVal, err := scan.iter.Item().Value()

	if err != nil {
		scan.item.err = err
		scan.item.regionError = util.RaftstoreErrToPbError(err)
		return
	}
	w, err  := ParseWrite(wVal)
	if err != nil {
		scan.item.err = err
		scan.item.regionError = util.RaftstoreErrToPbError(err)
		return
	}
	if w.Kind != WriteKindPut {
		// seek the next key
		scan.iter.Seek(EncodeKey(curKey, TsLast))
		scan.NextByIter()
		return
	}

	valKey := EncodeKey(curKey, w.StartTS)
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, valKey)
	if err != nil {
		scan.item.err = err
		scan.item.regionError = util.RaftstoreErrToPbError(err)
		return
	}


	scan.item.key = valKey
	scan.item.value = value
	lock,err := scan.txn.GetLock(curKey)
	if err != nil {
		scan.item.err = err
		scan.item.regionError = util.RaftstoreErrToPbError(err)
		return
	}


	if lock != nil && lock.Ts != scan.txn.StartTS {
		scan.item.keyError = KeyError{kvrpcpb.KeyError{
			Locked: lock.Info(curKey),
		}}
	}
	scan.count++
	// Get the next key
	scan.iter.Seek(EncodeKey(curKey, TsLast))
	return

}


type ScannerItem struct {
	key []byte
	value []byte
	keyError KeyError

	// if other err occurred, it will be saved in the regionError.Message.
	regionError *errorpb.Error

	// use for storing the error, just for test
	err error
}

func (it ScannerItem) Key() []byte {
	return it.key
}
func (it ScannerItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, it.key)
}
func (it ScannerItem) Value() []byte {
	return it.value
}
func (it ScannerItem) ValueSize() int {
	return len(it.value)
}
func (it ScannerItem) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, it.value), nil
}
func (it ScannerItem) KeyError() KeyError {
	return it.keyError
}
func (it ScannerItem) RegionError() *errorpb.Error {
	return it.regionError
}
