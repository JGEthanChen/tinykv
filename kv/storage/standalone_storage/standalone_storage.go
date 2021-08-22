package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	// db need a path to create, this mainly from config
	dbPath string
}



func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Data Here (1).
	dbPath := filepath.Join(conf.DBPath, "AloneStorage")
	return &StandAloneStorage{nil,dbPath}
}

func (s *StandAloneStorage) Start() error {
	// Your Data Here (1).
	// DB create when it start
	s.db = engine_util.CreateDB(s.dbPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Data Here (1).
	if err := s.db.Close();err!=nil{
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Data Here (1).

	// set up the transaction ,then return a struct,
	// AloneStorageReader for reading
	return &AloneStorageReader{
		txn:s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Data Here (1).
	// By using write batch, it can write all entries into DB together,
	// it will be more efficient.
	wb := new(engine_util.WriteBatch)
	for _,m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	return wb.WriteToDB(s.db)
}

type AloneStorageReader struct {
	// Your Data Here (1).
	txn *badger.Txn
}

func (a *AloneStorageReader)GetCF(cf string, key []byte) ([]byte, error) {
	// Your Data Here (1).
	val,err := engine_util.GetCFFromTxn(a.txn, cf, key)
	if err != nil && err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (a *AloneStorageReader)IterCF(cf string) engine_util.DBIterator {
	// Your Data Here (1).
	return engine_util.NewCFIterator(cf, a.txn)
}

func (a *AloneStorageReader)Close() {
	// Your Data Here (1).
	a.txn.Discard()
}