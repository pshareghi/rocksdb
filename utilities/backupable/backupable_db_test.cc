//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <algorithm>
#include <iostream>

#include "port/port.h"
#include "rocksdb/types.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/utilities/backupable_db.h"
#include "util/testharness.h"
#include "util/random.h"
#include "util/mutexlock.h"
#include "util/testutil.h"
#include "util/auto_roll_logger.h"

namespace rocksdb {

namespace {

using std::unique_ptr;

class DummyDB : public StackableDB {
 public:
  /* implicit */
  DummyDB(const Options& options, const std::string& dbname)
     : StackableDB(nullptr), options_(options), dbname_(dbname),
       deletions_enabled_(true), sequence_number_(0) {}

  virtual SequenceNumber GetLatestSequenceNumber() const override {
    return ++sequence_number_;
  }

  virtual const std::string& GetName() const override {
    return dbname_;
  }

  virtual Env* GetEnv() const override {
    return options_.env;
  }

  using DB::GetOptions;
  virtual const Options& GetOptions(ColumnFamilyHandle* column_family) const
      override {
    return options_;
  }

  virtual Status EnableFileDeletions(bool force) override {
    ASSERT_TRUE(!deletions_enabled_);
    deletions_enabled_ = true;
    return Status::OK();
  }

  virtual Status DisableFileDeletions() override {
    ASSERT_TRUE(deletions_enabled_);
    deletions_enabled_ = false;
    return Status::OK();
  }

  virtual Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
                              bool flush_memtable = true) override {
    ASSERT_TRUE(!deletions_enabled_);
    vec = live_files_;
    *mfs = 100;
    return Status::OK();
  }

  virtual ColumnFamilyHandle* DefaultColumnFamily() const override {
    return nullptr;
  }

  class DummyLogFile : public LogFile {
   public:
    /* implicit */
     DummyLogFile(const std::string& path, bool alive = true)
         : path_(path), alive_(alive) {}

    virtual std::string PathName() const override {
      return path_;
    }

    virtual uint64_t LogNumber() const override {
      // what business do you have calling this method?
      ASSERT_TRUE(false);
      return 0;
    }

    virtual WalFileType Type() const override {
      return alive_ ? kAliveLogFile : kArchivedLogFile;
    }

    virtual SequenceNumber StartSequence() const override {
      // backupabledb should not need this method
      ASSERT_TRUE(false);
      return 0;
    }

    virtual uint64_t SizeFileBytes() const override {
      // backupabledb should not need this method
      ASSERT_TRUE(false);
      return 0;
    }

   private:
    std::string path_;
    bool alive_;
  }; // DummyLogFile

  virtual Status GetSortedWalFiles(VectorLogPtr& files) override {
    ASSERT_TRUE(!deletions_enabled_);
    files.resize(wal_files_.size());
    for (size_t i = 0; i < files.size(); ++i) {
      files[i].reset(
          new DummyLogFile(wal_files_[i].first, wal_files_[i].second));
    }
    return Status::OK();
  }

  std::vector<std::string> live_files_;
  // pair<filename, alive?>
  std::vector<std::pair<std::string, bool>> wal_files_;
 private:
  Options options_;
  std::string dbname_;
  bool deletions_enabled_;
  mutable SequenceNumber sequence_number_;
}; // DummyDB

class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(Env* t) : EnvWrapper(t) {}

  class DummySequentialFile : public SequentialFile {
   public:
    DummySequentialFile() : SequentialFile(), rnd_(5) {}
    virtual Status Read(size_t n, Slice* result, char* scratch) override {
      size_t read_size = (n > size_left) ? size_left : n;
      for (size_t i = 0; i < read_size; ++i) {
        scratch[i] = rnd_.Next() & 255;
      }
      *result = Slice(scratch, read_size);
      size_left -= read_size;
      return Status::OK();
    }

    virtual Status Skip(uint64_t n) override {
      size_left = (n > size_left) ? size_left - n : 0;
      return Status::OK();
    }
   private:
    size_t size_left = 200;
    Random rnd_;
  };

  Status NewSequentialFile(const std::string& f, unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    MutexLock l(&mutex_);
    if (dummy_sequential_file_) {
      r->reset(new TestEnv::DummySequentialFile());
      return Status::OK();
    } else {
      return EnvWrapper::NewSequentialFile(f, r, options);
    }
  }

  Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    MutexLock l(&mutex_);
    written_files_.push_back(f);
    if (limit_written_files_ <= 0) {
      return Status::NotSupported("Sorry, can't do this");
    }
    limit_written_files_--;
    return EnvWrapper::NewWritableFile(f, r, options);
  }

  virtual Status DeleteFile(const std::string& fname) override {
    MutexLock l(&mutex_);
    ASSERT_GT(limit_delete_files_, 0U);
    limit_delete_files_--;
    return EnvWrapper::DeleteFile(fname);
  }

  void AssertWrittenFiles(std::vector<std::string>& should_have_written) {
    MutexLock l(&mutex_);
    sort(should_have_written.begin(), should_have_written.end());
    sort(written_files_.begin(), written_files_.end());
    ASSERT_TRUE(written_files_ == should_have_written);
  }

  void ClearWrittenFiles() {
    MutexLock l(&mutex_);
    written_files_.clear();
  }

  void SetLimitWrittenFiles(uint64_t limit) {
    MutexLock l(&mutex_);
    limit_written_files_ = limit;
  }

  void SetLimitDeleteFiles(uint64_t limit) {
    MutexLock l(&mutex_);
    limit_delete_files_ = limit;
  }

  void SetDummySequentialFile(bool dummy_sequential_file) {
    MutexLock l(&mutex_);
    dummy_sequential_file_ = dummy_sequential_file;
  }

 private:
  port::Mutex mutex_;
  bool dummy_sequential_file_ = false;
  std::vector<std::string> written_files_;
  uint64_t limit_written_files_ = 1000000;
  uint64_t limit_delete_files_ = 1000000;
};  // TestEnv

class FileManager : public EnvWrapper {
 public:
  explicit FileManager(Env* t) : EnvWrapper(t), rnd_(5) {}

  Status DeleteRandomFileInDir(const std::string& dir) {
    std::vector<std::string> children;
    GetChildren(dir, &children);
    if (children.size() <= 2) { // . and ..
      return Status::NotFound("");
    }
    while (true) {
      int i = rnd_.Next() % children.size();
      if (children[i] != "." && children[i] != "..") {
        return DeleteFile(dir + "/" + children[i]);
      }
    }
    // should never get here
    assert(false);
    return Status::NotFound("");
  }

  Status CorruptFile(const std::string& fname, uint64_t bytes_to_corrupt) {
    uint64_t size;
    Status s = GetFileSize(fname, &size);
    if (!s.ok()) {
      return s;
    }
    unique_ptr<RandomRWFile> file;
    EnvOptions env_options;
    env_options.use_mmap_writes = false;
    s = NewRandomRWFile(fname, &file, env_options);
    if (!s.ok()) {
      return s;
    }

    for (uint64_t i = 0; s.ok() && i < bytes_to_corrupt; ++i) {
      std::string tmp;
      // write one random byte to a random position
      s = file->Write(rnd_.Next() % size, test::RandomString(&rnd_, 1, &tmp));
    }
    return s;
  }

  Status CorruptChecksum(const std::string& fname, bool appear_valid) {
    std::string metadata;
    Status s = ReadFileToString(this, fname, &metadata);
    if (!s.ok()) {
      return s;
    }
    s = DeleteFile(fname);
    if (!s.ok()) {
      return s;
    }

    auto pos = metadata.find("private");
    if (pos == std::string::npos) {
      return Status::Corruption("private file is expected");
    }
    pos = metadata.find(" crc32 ", pos + 6);
    if (pos == std::string::npos) {
      return Status::Corruption("checksum not found");
    }

    if (metadata.size() < pos + 7) {
      return Status::Corruption("bad CRC32 checksum value");
    }

    if (appear_valid) {
      if (metadata[pos + 8] == '\n') {
        // single digit value, safe to insert one more digit
        metadata.insert(pos + 8, 1, '0');
      } else {
        metadata.erase(pos + 8, 1);
      }
    } else {
      metadata[pos + 7] = 'a';
    }

    return WriteToFile(fname, metadata);
  }

  Status WriteToFile(const std::string& fname, const std::string& data) {
    unique_ptr<WritableFile> file;
    EnvOptions env_options;
    env_options.use_mmap_writes = false;
    Status s = EnvWrapper::NewWritableFile(fname, &file, env_options);
    if (!s.ok()) {
      return s;
    }
    return file->Append(Slice(data));
  }

 private:
  Random rnd_;
}; // FileManager

// utility functions
static size_t FillDB(DB* db, int from, int to) {
  size_t bytes_written = 0;
  for (int i = from; i < to; ++i) {
    std::string key = "testkey" + std::to_string(i);
    std::string value = "testvalue" + std::to_string(i);
    bytes_written += key.size() + value.size();

    ASSERT_OK(db->Put(WriteOptions(), Slice(key), Slice(value)));
  }
  return bytes_written;
}

static void AssertExists(DB* db, int from, int to) {
  for (int i = from; i < to; ++i) {
    std::string key = "testkey" + std::to_string(i);
    std::string value;
    Status s = db->Get(ReadOptions(), Slice(key), &value);
    ASSERT_EQ(value, "testvalue" + std::to_string(i));
  }
}

static void AssertEmpty(DB* db, int from, int to) {
  for (int i = from; i < to; ++i) {
    std::string key = "testkey" + std::to_string(i);
    std::string value = "testvalue" + std::to_string(i);

    Status s = db->Get(ReadOptions(), Slice(key), &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}

class BackupableDBTest {
 public:
  BackupableDBTest() {
    // set up files
    dbname_ = test::TmpDir() + "/backupable_db";
    backupdir_ = test::TmpDir() + "/backupable_db_backup";

    // set up envs
    env_ = Env::Default();
    test_db_env_.reset(new TestEnv(env_));
    test_backup_env_.reset(new TestEnv(env_));
    file_manager_.reset(new FileManager(env_));

    // set up db options
    options_.create_if_missing = true;
    options_.paranoid_checks = true;
    options_.write_buffer_size = 1 << 17; // 128KB
    options_.env = test_db_env_.get();
    options_.wal_dir = dbname_;
    // set up backup db options
    CreateLoggerFromOptions(dbname_, backupdir_, env_,
                            DBOptions(), &logger_);
    backupable_options_.reset(new BackupableDBOptions(
        backupdir_, test_backup_env_.get(), true, logger_.get(), true));

    // delete old files in db
    DestroyDB(dbname_, Options());
  }

  DB* OpenDB() {
    DB* db;
    ASSERT_OK(DB::Open(options_, dbname_, &db));
    return db;
  }

  void OpenBackupableDB(bool destroy_old_data = false, bool dummy = false,
                        bool share_table_files = true,
                        bool share_with_checksums = false) {
    // reset all the defaults
    test_backup_env_->SetLimitWrittenFiles(1000000);
    test_db_env_->SetLimitWrittenFiles(1000000);
    test_db_env_->SetDummySequentialFile(dummy);

    DB* db;
    if (dummy) {
      dummy_db_ = new DummyDB(options_, dbname_);
      db = dummy_db_;
    } else {
      ASSERT_OK(DB::Open(options_, dbname_, &db));
    }
    backupable_options_->destroy_old_data = destroy_old_data;
    backupable_options_->share_table_files = share_table_files;
    backupable_options_->share_files_with_checksum = share_with_checksums;
    db_.reset(new BackupableDB(db, *backupable_options_));
  }

  void CloseBackupableDB() {
    db_.reset(nullptr);
  }

  void OpenRestoreDB() {
    backupable_options_->destroy_old_data = false;
    restore_db_.reset(
        new RestoreBackupableDB(test_db_env_.get(), *backupable_options_));
  }

  void CloseRestoreDB() {
    restore_db_.reset(nullptr);
  }

  // restores backup backup_id and asserts the existence of
  // [start_exist, end_exist> and not-existence of
  // [end_exist, end>
  //
  // if backup_id == 0, it means restore from latest
  // if end == 0, don't check AssertEmpty
  void AssertBackupConsistency(BackupID backup_id, uint32_t start_exist,
                               uint32_t end_exist, uint32_t end = 0,
                               bool keep_log_files = false) {
    RestoreOptions restore_options(keep_log_files);
    bool opened_restore = false;
    if (restore_db_.get() == nullptr) {
      opened_restore = true;
      OpenRestoreDB();
    }
    if (backup_id > 0) {
      ASSERT_OK(restore_db_->RestoreDBFromBackup(backup_id, dbname_, dbname_,
                                                 restore_options));
    } else {
      ASSERT_OK(restore_db_->RestoreDBFromLatestBackup(dbname_, dbname_,
                                                       restore_options));
    }
    DB* db = OpenDB();
    AssertExists(db, start_exist, end_exist);
    if (end != 0) {
      AssertEmpty(db, end_exist, end);
    }
    delete db;
    if (opened_restore) {
      CloseRestoreDB();
    }
  }

  void DeleteLogFiles() {
    std::vector<std::string> delete_logs;
    env_->GetChildren(dbname_, &delete_logs);
    for (auto f : delete_logs) {
      uint64_t number;
      FileType type;
      bool ok = ParseFileName(f, &number, &type);
      if (ok && type == kLogFile) {
        env_->DeleteFile(dbname_ + "/" + f);
      }
    }
  }

  // files
  std::string dbname_;
  std::string backupdir_;

  // envs
  Env* env_;
  unique_ptr<TestEnv> test_db_env_;
  unique_ptr<TestEnv> test_backup_env_;
  unique_ptr<FileManager> file_manager_;

  // all the dbs!
  DummyDB* dummy_db_; // BackupableDB owns dummy_db_
  unique_ptr<BackupableDB> db_;
  unique_ptr<RestoreBackupableDB> restore_db_;

  // options
  Options options_;
  unique_ptr<BackupableDBOptions> backupable_options_;
  std::shared_ptr<Logger> logger_;
}; // BackupableDBTest

void AppendPath(const std::string& path, std::vector<std::string>& v) {
  for (auto& f : v) {
    f = path + f;
  }
}

// this will make sure that backup does not copy the same file twice
TEST(BackupableDBTest, NoDoubleCopy) {
  OpenBackupableDB(true, true);

  // should write 5 DB files + LATEST_BACKUP + one meta file
  test_backup_env_->SetLimitWrittenFiles(7);
  test_backup_env_->ClearWrittenFiles();
  test_db_env_->SetLimitWrittenFiles(0);
  dummy_db_->live_files_ = { "/00010.sst", "/00011.sst",
                             "/CURRENT",   "/MANIFEST-01" };
  dummy_db_->wal_files_ = {{"/00011.log", true}, {"/00012.log", false}};
  ASSERT_OK(db_->CreateNewBackup(false));
  std::vector<std::string> should_have_written = {
    "/shared/00010.sst.tmp",
    "/shared/00011.sst.tmp",
    "/private/1.tmp/CURRENT",
    "/private/1.tmp/MANIFEST-01",
    "/private/1.tmp/00011.log",
    "/meta/1.tmp",
    "/LATEST_BACKUP.tmp"
  };
  AppendPath(dbname_ + "_backup", should_have_written);
  test_backup_env_->AssertWrittenFiles(should_have_written);

  // should write 4 new DB files + LATEST_BACKUP + one meta file
  // should not write/copy 00010.sst, since it's already there!
  test_backup_env_->SetLimitWrittenFiles(6);
  test_backup_env_->ClearWrittenFiles();
  dummy_db_->live_files_ = { "/00010.sst", "/00015.sst",
                             "/CURRENT",   "/MANIFEST-01" };
  dummy_db_->wal_files_ = {{"/00011.log", true}, {"/00012.log", false}};
  ASSERT_OK(db_->CreateNewBackup(false));
  // should not open 00010.sst - it's already there
  should_have_written = {
    "/shared/00015.sst.tmp",
    "/private/2.tmp/CURRENT",
    "/private/2.tmp/MANIFEST-01",
    "/private/2.tmp/00011.log",
    "/meta/2.tmp",
    "/LATEST_BACKUP.tmp"
  };
  AppendPath(dbname_ + "_backup", should_have_written);
  test_backup_env_->AssertWrittenFiles(should_have_written);

  ASSERT_OK(db_->DeleteBackup(1));
  ASSERT_EQ(true,
            test_backup_env_->FileExists(backupdir_ + "/shared/00010.sst"));
  // 00011.sst was only in backup 1, should be deleted
  ASSERT_EQ(false,
            test_backup_env_->FileExists(backupdir_ + "/shared/00011.sst"));
  ASSERT_EQ(true,
            test_backup_env_->FileExists(backupdir_ + "/shared/00015.sst"));

  // MANIFEST file size should be only 100
  uint64_t size;
  test_backup_env_->GetFileSize(backupdir_ + "/private/2/MANIFEST-01", &size);
  ASSERT_EQ(100UL, size);
  test_backup_env_->GetFileSize(backupdir_ + "/shared/00015.sst", &size);
  ASSERT_EQ(200UL, size);

  CloseBackupableDB();
}

// test various kind of corruptions that may happen:
// 1. Not able to write a file for backup - that backup should fail,
//      everything else should work
// 2. Corrupted/deleted LATEST_BACKUP - everything should work fine
// 3. Corrupted backup meta file or missing backuped file - we should
//      not be able to open that backup, but all other backups should be
//      fine
// 4. Corrupted checksum value - if the checksum is not a valid uint32_t,
//      db open should fail, otherwise, it aborts during the restore process.
TEST(BackupableDBTest, CorruptionsTest) {
  const int keys_iteration = 5000;
  Random rnd(6);
  Status s;

  OpenBackupableDB(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(!!(rnd.Next() % 2)));
  }

  // ---------- case 1. - fail a write -----------
  // try creating backup 6, but fail a write
  FillDB(db_.get(), keys_iteration * 5, keys_iteration * 6);
  test_backup_env_->SetLimitWrittenFiles(2);
  // should fail
  s = db_->CreateNewBackup(!!(rnd.Next() % 2));
  ASSERT_TRUE(!s.ok());
  test_backup_env_->SetLimitWrittenFiles(1000000);
  // latest backup should have all the keys
  CloseBackupableDB();
  AssertBackupConsistency(0, 0, keys_iteration * 5, keys_iteration * 6);

  // ---------- case 2. - corrupt/delete latest backup -----------
  ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/LATEST_BACKUP", 2));
  AssertBackupConsistency(0, 0, keys_iteration * 5);
  ASSERT_OK(file_manager_->DeleteFile(backupdir_ + "/LATEST_BACKUP"));
  AssertBackupConsistency(0, 0, keys_iteration * 5);
  // create backup 6, point LATEST_BACKUP to 5
  OpenBackupableDB();
  FillDB(db_.get(), keys_iteration * 5, keys_iteration * 6);
  ASSERT_OK(db_->CreateNewBackup(false));
  CloseBackupableDB();
  ASSERT_OK(file_manager_->WriteToFile(backupdir_ + "/LATEST_BACKUP", "5"));
  AssertBackupConsistency(0, 0, keys_iteration * 5, keys_iteration * 6);
  // assert that all 6 data is gone!
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/6") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/6") == false);

  // --------- case 3. corrupted backup meta or missing backuped file ----
  ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/meta/5", 3));
  // since 5 meta is now corrupted, latest backup should be 4
  AssertBackupConsistency(0, 0, keys_iteration * 4, keys_iteration * 5);
  OpenRestoreDB();
  s = restore_db_->RestoreDBFromBackup(5, dbname_, dbname_);
  ASSERT_TRUE(!s.ok());
  CloseRestoreDB();
  ASSERT_OK(file_manager_->DeleteRandomFileInDir(backupdir_ + "/private/4"));
  // 4 is corrupted, 3 is the latest backup now
  AssertBackupConsistency(0, 0, keys_iteration * 3, keys_iteration * 5);
  OpenRestoreDB();
  s = restore_db_->RestoreDBFromBackup(4, dbname_, dbname_);
  CloseRestoreDB();
  ASSERT_TRUE(!s.ok());

  // --------- case 4. corrupted checksum value ----
  ASSERT_OK(file_manager_->CorruptChecksum(backupdir_ + "/meta/3", false));
  // checksum of backup 3 is an invalid value, this can be detected at
  // db open time, and it reverts to the previous backup automatically
  AssertBackupConsistency(0, 0, keys_iteration * 2, keys_iteration * 5);
  // checksum of the backup 2 appears to be valid, this can cause checksum
  // mismatch and abort restore process
  ASSERT_OK(file_manager_->CorruptChecksum(backupdir_ + "/meta/2", true));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/2"));
  OpenRestoreDB();
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/2"));
  s = restore_db_->RestoreDBFromBackup(2, dbname_, dbname_);
  ASSERT_TRUE(!s.ok());

  // make sure that no corrupt backups have actually been deleted!
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/1"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/2"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/3"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/4"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/5"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/1"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/2"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/3"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/4"));
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/5"));

  // delete the corrupt backups and then make sure they're actually deleted
  ASSERT_OK(restore_db_->DeleteBackup(5));
  ASSERT_OK(restore_db_->DeleteBackup(4));
  ASSERT_OK(restore_db_->DeleteBackup(3));
  ASSERT_OK(restore_db_->DeleteBackup(2));
  (void) restore_db_->GarbageCollect();
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/5") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/5") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/4") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/4") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/3") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/3") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/2") == false);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/2") == false);

  CloseRestoreDB();
  AssertBackupConsistency(0, 0, keys_iteration * 1, keys_iteration * 5);

  // new backup should be 2!
  OpenBackupableDB();
  FillDB(db_.get(), keys_iteration * 1, keys_iteration * 2);
  ASSERT_OK(db_->CreateNewBackup(!!(rnd.Next() % 2)));
  CloseBackupableDB();
  AssertBackupConsistency(2, 0, keys_iteration * 2, keys_iteration * 5);
}

// This test verifies we don't delete the latest backup when read-only option is
// set
TEST(BackupableDBTest, NoDeleteWithReadOnly) {
  const int keys_iteration = 5000;
  Random rnd(6);
  Status s;

  OpenBackupableDB(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(!!(rnd.Next() % 2)));
  }
  CloseBackupableDB();
  ASSERT_OK(file_manager_->WriteToFile(backupdir_ + "/LATEST_BACKUP", "4"));

  backupable_options_->destroy_old_data = false;
  BackupEngineReadOnly* read_only_backup_engine;
  ASSERT_OK(BackupEngineReadOnly::Open(env_, *backupable_options_,
                                       &read_only_backup_engine));

  // assert that data from backup 5 is still here (even though LATEST_BACKUP
  // says 4 is latest)
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/meta/5") == true);
  ASSERT_TRUE(file_manager_->FileExists(backupdir_ + "/private/5") == true);

  // even though 5 is here, we should only see 4 backups
  std::vector<BackupInfo> backup_info;
  read_only_backup_engine->GetBackupInfo(&backup_info);
  ASSERT_EQ(4UL, backup_info.size());
  delete read_only_backup_engine;
}

// open DB, write, close DB, backup, restore, repeat
TEST(BackupableDBTest, OfflineIntegrationTest) {
  // has to be a big number, so that it triggers the memtable flush
  const int keys_iteration = 5000;
  const int max_key = keys_iteration * 4 + 10;
  // first iter -- flush before backup
  // second iter -- don't flush before backup
  for (int iter = 0; iter < 2; ++iter) {
    // delete old data
    DestroyDB(dbname_, Options());
    bool destroy_data = true;

    // every iteration --
    // 1. insert new data in the DB
    // 2. backup the DB
    // 3. destroy the db
    // 4. restore the db, check everything is still there
    for (int i = 0; i < 5; ++i) {
      // in last iteration, put smaller amount of data,
      int fill_up_to = std::min(keys_iteration * (i + 1), max_key);
      // ---- insert new data and back up ----
      OpenBackupableDB(destroy_data);
      destroy_data = false;
      FillDB(db_.get(), keys_iteration * i, fill_up_to);
      ASSERT_OK(db_->CreateNewBackup(iter == 0));
      CloseBackupableDB();
      DestroyDB(dbname_, Options());

      // ---- make sure it's empty ----
      DB* db = OpenDB();
      AssertEmpty(db, 0, fill_up_to);
      delete db;

      // ---- restore the DB ----
      OpenRestoreDB();
      if (i >= 3) { // test purge old backups
        // when i == 4, purge to only 1 backup
        // when i == 3, purge to 2 backups
        ASSERT_OK(restore_db_->PurgeOldBackups(5 - i));
      }
      // ---- make sure the data is there ---
      AssertBackupConsistency(0, 0, fill_up_to, max_key);
      CloseRestoreDB();
    }
  }
}

// open DB, write, backup, write, backup, close, restore
TEST(BackupableDBTest, OnlineIntegrationTest) {
  // has to be a big number, so that it triggers the memtable flush
  const int keys_iteration = 5000;
  const int max_key = keys_iteration * 4 + 10;
  Random rnd(7);
  // delete old data
  DestroyDB(dbname_, Options());

  OpenBackupableDB(true);
  // write some data, backup, repeat
  for (int i = 0; i < 5; ++i) {
    if (i == 4) {
      // delete backup number 2, online delete!
      OpenRestoreDB();
      ASSERT_OK(restore_db_->DeleteBackup(2));
      CloseRestoreDB();
    }
    // in last iteration, put smaller amount of data,
    // so that backups can share sst files
    int fill_up_to = std::min(keys_iteration * (i + 1), max_key);
    FillDB(db_.get(), keys_iteration * i, fill_up_to);
    // we should get consistent results with flush_before_backup
    // set to both true and false
    ASSERT_OK(db_->CreateNewBackup(!!(rnd.Next() % 2)));
  }
  // close and destroy
  CloseBackupableDB();
  DestroyDB(dbname_, Options());

  // ---- make sure it's empty ----
  DB* db = OpenDB();
  AssertEmpty(db, 0, max_key);
  delete db;

  // ---- restore every backup and verify all the data is there ----
  OpenRestoreDB();
  for (int i = 1; i <= 5; ++i) {
    if (i == 2) {
      // we deleted backup 2
      Status s = restore_db_->RestoreDBFromBackup(2, dbname_, dbname_);
      ASSERT_TRUE(!s.ok());
    } else {
      int fill_up_to = std::min(keys_iteration * i, max_key);
      AssertBackupConsistency(i, 0, fill_up_to, max_key);
    }
  }

  // delete some backups -- this should leave only backups 3 and 5 alive
  ASSERT_OK(restore_db_->DeleteBackup(4));
  ASSERT_OK(restore_db_->PurgeOldBackups(2));

  std::vector<BackupInfo> backup_info;
  restore_db_->GetBackupInfo(&backup_info);
  ASSERT_EQ(2UL, backup_info.size());

  // check backup 3
  AssertBackupConsistency(3, 0, 3 * keys_iteration, max_key);
  // check backup 5
  AssertBackupConsistency(5, 0, max_key);

  CloseRestoreDB();
}

TEST(BackupableDBTest, FailOverwritingBackups) {
  options_.write_buffer_size = 1024 * 1024 * 1024;  // 1GB
  // create backups 1, 2, 3, 4, 5
  OpenBackupableDB(true);
  for (int i = 0; i < 5; ++i) {
    CloseBackupableDB();
    DeleteLogFiles();
    OpenBackupableDB(false);
    FillDB(db_.get(), 100 * i, 100 * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(true));
  }
  CloseBackupableDB();

  // restore 3
  OpenRestoreDB();
  ASSERT_OK(restore_db_->RestoreDBFromBackup(3, dbname_, dbname_));
  CloseRestoreDB();

  OpenBackupableDB(false);
  FillDB(db_.get(), 0, 300);
  Status s = db_->CreateNewBackup(true);
  // the new backup fails because new table files
  // clash with old table files from backups 4 and 5
  // (since write_buffer_size is huge, we can be sure that
  // each backup will generate only one sst file and that
  // a file generated by a new backup is the same as
  // sst file generated by backup 4)
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_OK(db_->DeleteBackup(4));
  ASSERT_OK(db_->DeleteBackup(5));
  // now, the backup can succeed
  ASSERT_OK(db_->CreateNewBackup(true));
  CloseBackupableDB();
}

TEST(BackupableDBTest, NoShareTableFiles) {
  const int keys_iteration = 5000;
  OpenBackupableDB(true, false, false);
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(!!(i % 2)));
  }
  CloseBackupableDB();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 6);
  }
}

// Verify that you can backup and restore with share_files_with_checksum on
TEST(BackupableDBTest, ShareTableFilesWithChecksums) {
  const int keys_iteration = 5000;
  OpenBackupableDB(true, false, true, true);
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(!!(i % 2)));
  }
  CloseBackupableDB();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 6);
  }
}

// Verify that you can backup and restore using share_files_with_checksum set to
// false and then transition this option to true
TEST(BackupableDBTest, ShareTableFilesWithChecksumsTransition) {
  const int keys_iteration = 5000;
  // set share_files_with_checksum to false
  OpenBackupableDB(true, false, true, false);
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(true));
  }
  CloseBackupableDB();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 6);
  }

  // set share_files_with_checksum to true and do some more backups
  OpenBackupableDB(true, false, true, true);
  for (int i = 5; i < 10; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(db_->CreateNewBackup(true));
  }
  CloseBackupableDB();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 5 + 1),
                            keys_iteration * 11);
  }
}

TEST(BackupableDBTest, DeleteTmpFiles) {
  OpenBackupableDB();
  CloseBackupableDB();
  std::string shared_tmp = backupdir_ + "/shared/00006.sst.tmp";
  std::string private_tmp_dir = backupdir_ + "/private/10.tmp";
  std::string private_tmp_file = private_tmp_dir + "/00003.sst";
  file_manager_->WriteToFile(shared_tmp, "tmp");
  file_manager_->CreateDir(private_tmp_dir);
  file_manager_->WriteToFile(private_tmp_file, "tmp");
  ASSERT_EQ(true, file_manager_->FileExists(private_tmp_dir));
  OpenBackupableDB();
  // Need to call this explicitly to delete tmp files
  (void) db_->GarbageCollect();
  CloseBackupableDB();
  ASSERT_EQ(false, file_manager_->FileExists(shared_tmp));
  ASSERT_EQ(false, file_manager_->FileExists(private_tmp_file));
  ASSERT_EQ(false, file_manager_->FileExists(private_tmp_dir));
}

TEST(BackupableDBTest, KeepLogFiles) {
  backupable_options_->backup_log_files = false;
  // basically infinite
  options_.WAL_ttl_seconds = 24 * 60 * 60;
  OpenBackupableDB(true);
  FillDB(db_.get(), 0, 100);
  ASSERT_OK(db_->Flush(FlushOptions()));
  FillDB(db_.get(), 100, 200);
  ASSERT_OK(db_->CreateNewBackup(false));
  FillDB(db_.get(), 200, 300);
  ASSERT_OK(db_->Flush(FlushOptions()));
  FillDB(db_.get(), 300, 400);
  ASSERT_OK(db_->Flush(FlushOptions()));
  FillDB(db_.get(), 400, 500);
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseBackupableDB();

  // all data should be there if we call with keep_log_files = true
  AssertBackupConsistency(0, 0, 500, 600, true);
}

TEST(BackupableDBTest, RateLimiting) {
  uint64_t const KB = 1024 * 1024;
  size_t const kMicrosPerSec = 1000 * 1000LL;

  std::vector<std::pair<uint64_t, uint64_t>> limits(
      {{KB, 5 * KB}, {2 * KB, 3 * KB}});

  for (const auto& limit : limits) {
    // destroy old data
    DestroyDB(dbname_, Options());

    backupable_options_->backup_rate_limit = limit.first;
    backupable_options_->restore_rate_limit = limit.second;
    options_.compression = kNoCompression;
    OpenBackupableDB(true);
    size_t bytes_written = FillDB(db_.get(), 0, 100000);

    auto start_backup = env_->NowMicros();
    ASSERT_OK(db_->CreateNewBackup(false));
    auto backup_time = env_->NowMicros() - start_backup;
    auto rate_limited_backup_time = (bytes_written * kMicrosPerSec) /
                                    backupable_options_->backup_rate_limit;
    ASSERT_GT(backup_time, 0.8 * rate_limited_backup_time);

    CloseBackupableDB();

    OpenRestoreDB();
    auto start_restore = env_->NowMicros();
    ASSERT_OK(restore_db_->RestoreDBFromLatestBackup(dbname_, dbname_));
    auto restore_time = env_->NowMicros() - start_restore;
    CloseRestoreDB();
    auto rate_limited_restore_time = (bytes_written * kMicrosPerSec) /
                                     backupable_options_->restore_rate_limit;
    ASSERT_GT(restore_time, 0.8 * rate_limited_restore_time);

    AssertBackupConsistency(0, 0, 100000, 100010);
  }
}

TEST(BackupableDBTest, ReadOnlyBackupEngine) {
  DestroyDB(dbname_, Options());
  OpenBackupableDB(true);
  FillDB(db_.get(), 0, 100);
  ASSERT_OK(db_->CreateNewBackup(true));
  FillDB(db_.get(), 100, 200);
  ASSERT_OK(db_->CreateNewBackup(true));
  CloseBackupableDB();
  DestroyDB(dbname_, Options());

  backupable_options_->destroy_old_data = false;
  test_backup_env_->ClearWrittenFiles();
  test_backup_env_->SetLimitDeleteFiles(0);
  BackupEngineReadOnly* read_only_backup_engine;
  ASSERT_OK(BackupEngineReadOnly::Open(env_, *backupable_options_,
                                       &read_only_backup_engine));
  std::vector<BackupInfo> backup_info;
  read_only_backup_engine->GetBackupInfo(&backup_info);
  ASSERT_EQ(backup_info.size(), 2U);

  RestoreOptions restore_options(false);
  ASSERT_OK(read_only_backup_engine->RestoreDBFromLatestBackup(
      dbname_, dbname_, restore_options));
  delete read_only_backup_engine;
  std::vector<std::string> should_have_written;
  test_backup_env_->AssertWrittenFiles(should_have_written);

  DB* db = OpenDB();
  AssertExists(db, 0, 200);
  delete db;
}

}  // anon namespace

} //  namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
