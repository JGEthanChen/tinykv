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
	AloneEngine *engine_util.Engines
}



func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	//mainly use kv in the engine , raft is not used
	kvpath := conf.DBPath
	raftpath := filepath.Join(conf.DBPath,"raft")
	kv := engine_util.CreateDB(kvpath,false)
	raft := engine_util.CreateDB(raftpath,true)
	return &StandAloneStorage{
		engine_util.NewEngines(kv,raft,kvpath,raftpath)}
}

func (s *StandAloneStorage) Start() error {
	// for a StandAloneStorage, no start process was required
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.AloneEngine.Close();err!=nil{
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	//set up the transaction ,then return a struct ,AloneStoraageReader,which is defined above

	return &AloneStorageReader{
		txn:s.AloneEngine.Kv.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)
	for _,m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	return s.AloneEngine.WriteKV(wb)
}

type AloneStorageReader struct {
	txn *badger.Txn
}

func (a *AloneStorageReader)GetCF(cf string, key []byte) ([]byte, error) {
	val,err := engine_util.GetCFFromTxn(a.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil,nil
	}
	if err != nil {
		return nil,err
	}
	return val,nil
}

func (a *AloneStorageReader)IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, a.txn)
}

func (a *AloneStorageReader)Close() {
	a.txn.Discard()
}