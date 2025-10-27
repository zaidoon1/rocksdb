//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class PrefixExistsTest : public testing::Test {
 public:
  PrefixExistsTest() : db_(nullptr) {}

  ~PrefixExistsTest() override {
    if (db_ != nullptr) {
      delete db_;
    }
  }

 protected:
  void SetUp() override {
    // Create a temporary directory for the test database
    dbname_ = test::PerThreadDBPath("prefix_exists_test");
    DestroyDB(dbname_, Options());
  }

  void TearDown() override {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
    DestroyDB(dbname_, Options());
  }

  void OpenDB(const Options& options = Options()) {
    Options opts = options;
    if (opts.create_if_missing == false) {
      opts.create_if_missing = true;
    }
    Status s = DB::Open(opts, dbname_, &db_);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(db_, nullptr);
  }

  DB* db_;
  std::string dbname_;
};

TEST_F(PrefixExistsTest, BasicPrefixExists) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  OpenDB(options);

  // Put some keys with the same prefix
  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc3", "value3"));

  // Put keys with different prefix
  ASSERT_OK(db_->Put(WriteOptions(), "def1", "value4"));
  ASSERT_OK(db_->Put(WriteOptions(), "def2", "value5"));

  ReadOptions ro;

  // Test prefix exists
  ASSERT_OK(db_->PrefixExists(ro, "abc1"));
  ASSERT_OK(db_->PrefixExists(ro, "abc2"));
  ASSERT_OK(db_->PrefixExists(ro, "def1"));

  // Test prefix doesn't exist
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz1").IsNotFound());
  ASSERT_TRUE(db_->PrefixExists(ro, "ghi1").IsNotFound());
}

TEST_F(PrefixExistsTest, PrefixExistsWithMemtable) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  // Put keys in memtable
  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aa2", "value2"));
  ASSERT_OK(db_->Put(WriteOptions(), "bb1", "value3"));

  ReadOptions ro;

  // Prefix should exist in memtable
  ASSERT_OK(db_->PrefixExists(ro, "aa1"));
  ASSERT_OK(db_->PrefixExists(ro, "aa2"));
  ASSERT_OK(db_->PrefixExists(ro, "bb1"));

  // Non-existent prefix
  ASSERT_TRUE(db_->PrefixExists(ro, "cc1").IsNotFound());
}

TEST_F(PrefixExistsTest, PrefixExistsWithFlush) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  // Put keys and flush to SST
  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aa2", "value2"));
  ASSERT_OK(db_->Put(WriteOptions(), "bb1", "value3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Put more keys in memtable
  ASSERT_OK(db_->Put(WriteOptions(), "cc1", "value4"));
  ASSERT_OK(db_->Put(WriteOptions(), "cc2", "value5"));

  ReadOptions ro;

  // Prefix should exist in both SST and memtable
  ASSERT_OK(db_->PrefixExists(ro, "aa1"));
  ASSERT_OK(db_->PrefixExists(ro, "bb1"));
  ASSERT_OK(db_->PrefixExists(ro, "cc1"));

  // Non-existent prefix
  ASSERT_TRUE(db_->PrefixExists(ro, "dd1").IsNotFound());
}

TEST_F(PrefixExistsTest, PrefixExistsWithoutPrefixExtractor) {
  Options options;
  // No prefix extractor - should behave like regular key lookup
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));

  ReadOptions ro;

  // Should find exact keys
  ASSERT_OK(db_->PrefixExists(ro, "key1"));
  ASSERT_OK(db_->PrefixExists(ro, "key2"));

  // Should not find non-existent keys
  ASSERT_TRUE(db_->PrefixExists(ro, "key3").IsNotFound());
}

TEST_F(PrefixExistsTest, PrefixExistsWithColumnFamily) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));

  // Create column family
  ColumnFamilyOptions cf_options = options;
  cf_options.prefix_extractor.reset(NewFixedPrefixTransform(2));

  OpenDB(options);

  ColumnFamilyHandle* cf_handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(cf_options, "test_cf", &cf_handle));

  // Put keys in default column family
  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));

  // Put keys in test column family
  ASSERT_OK(db_->Put(WriteOptions(), cf_handle, "bb1", "value2"));

  ReadOptions ro;

  // Check default column family
  ASSERT_OK(db_->PrefixExists(ro, nullptr, "aa1"));
  ASSERT_TRUE(db_->PrefixExists(ro, nullptr, "bb1").IsNotFound());

  // Check test column family
  ASSERT_OK(db_->PrefixExists(ro, cf_handle, "bb1"));
  ASSERT_TRUE(db_->PrefixExists(ro, cf_handle, "aa1").IsNotFound());

  delete cf_handle;
}

TEST_F(PrefixExistsTest, PrefixExistsPerformance) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  OpenDB(options);

  // Insert many keys
  const int num_keys = 10000;
  for (int i = 0; i < num_keys; ++i) {
    std::string key = "key" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;

  // Measure time for prefix exists checks
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 1000; ++i) {
    std::string key = "key" + std::to_string(i);
    Status s = db_->PrefixExists(ro, key);
    ASSERT_TRUE(s.ok() || s.IsNotFound());
  }
  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  // Should be very fast - less than 100 microseconds per check on average
  ASSERT_LT(duration.count(), 1000000);  // 1 second for 1000 checks
}

TEST_F(PrefixExistsTest, PrefixExistsWithDeletes) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  // Put and delete keys
  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aa2", "value2"));
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));

  ReadOptions ro;

  // aa1 is deleted but aa2 still exists
  ASSERT_TRUE(db_->PrefixExists(ro, "aa1").IsNotFound());
  ASSERT_OK(db_->PrefixExists(ro, "aa2"));
}

TEST_F(PrefixExistsTest, PrefixExistsWithSnapshot) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));

  const Snapshot* snapshot = db_->GetSnapshot();

  // Add more data after snapshot
  ASSERT_OK(db_->Put(WriteOptions(), "aa2", "value2"));

  ReadOptions ro;
  ro.snapshot = snapshot;

  // Should only see data from snapshot
  ASSERT_OK(db_->PrefixExists(ro, "aa1"));
  ASSERT_TRUE(db_->PrefixExists(ro, "aa2").IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(PrefixExistsTest, PrefixExistsEmptyDatabase) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  ReadOptions ro;

  // Should not find anything in empty database
  ASSERT_TRUE(db_->PrefixExists(ro, "aa1").IsNotFound());
  ASSERT_TRUE(db_->PrefixExists(ro, "bb1").IsNotFound());
}

TEST_F(PrefixExistsTest, PrefixExistsWithMerge) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "aa1", "value2"));

  ReadOptions ro;

  // Should find merged key
  ASSERT_OK(db_->PrefixExists(ro, "aa1"));
}

TEST_F(PrefixExistsTest, MemtablePrefixBloomEnabled) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  // Put keys in memtable
  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));

  ReadOptions ro;

  // Should find keys (bloom filter says maybe exists, seek confirms)
  ASSERT_OK(db_->PrefixExists(ro, "abc1"));
  ASSERT_OK(db_->PrefixExists(ro, "abc2"));

  // Should not find non-existent prefix (bloom filter says doesn't exist)
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz1").IsNotFound());
}

TEST_F(PrefixExistsTest, MemtablePrefixBloomDisabled) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_prefix_bloom_size_ratio = 0.0;  // Disabled
  OpenDB(options);

  // Put keys in memtable
  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));

  ReadOptions ro;

  // Should still work (fallback to seek without bloom filter)
  ASSERT_OK(db_->PrefixExists(ro, "abc1"));
  ASSERT_OK(db_->PrefixExists(ro, "abc2"));
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz1").IsNotFound());
}

TEST_F(PrefixExistsTest, MemtablePrefixBloomFalsePositive) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  // Put key "abc1"
  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));

  ReadOptions ro;

  // Bloom filter may have false positives, but seek should find correct result
  // This tests that we don't rely solely on bloom filter
  ASSERT_OK(db_->PrefixExists(ro, "abc1"));
  ASSERT_TRUE(db_->PrefixExists(ro, "def1").IsNotFound());
}

TEST_F(PrefixExistsTest, NoExtractorWithMemtablePrefixBloom) {
  Options options;
  // No prefix extractor
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));

  ReadOptions ro;

  // Should work with full key as prefix
  ASSERT_OK(db_->PrefixExists(ro, "key1"));
  ASSERT_OK(db_->PrefixExists(ro, "key2"));
  ASSERT_TRUE(db_->PrefixExists(ro, "key3").IsNotFound());
}

TEST_F(PrefixExistsTest, MultipleImmutableMemtables) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.write_buffer_size = 1024;  // Small buffer to force multiple memtables
  OpenDB(options);

  // Fill first memtable
  for (int i = 0; i < 100; ++i) {
    std::string key = "aa" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
  }

  // Fill second memtable (first becomes immutable)
  for (int i = 0; i < 100; ++i) {
    std::string key = "bb" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
  }

  // Fill third memtable (second becomes immutable)
  for (int i = 0; i < 100; ++i) {
    std::string key = "cc" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
  }

  ReadOptions ro;

  // Should find keys in all memtables
  ASSERT_OK(db_->PrefixExists(ro, "aa0"));
  ASSERT_OK(db_->PrefixExists(ro, "bb0"));
  ASSERT_OK(db_->PrefixExists(ro, "cc0"));
}

TEST_F(PrefixExistsTest, KeyInFirstImmutableMemtable) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.write_buffer_size = 1024;
  OpenDB(options);

  // Fill and flush first memtable
  for (int i = 0; i < 100; ++i) {
    std::string key = "aa" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
  }

  // Fill second memtable (first becomes immutable)
  for (int i = 0; i < 100; ++i) {
    std::string key = "bb" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
  }

  ReadOptions ro;

  // Should find key in first immutable memtable
  ASSERT_OK(db_->PrefixExists(ro, "aa0"));
}

TEST_F(PrefixExistsTest, BloomFilterInSST) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));

  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  OpenDB(options);

  // Put and flush to SST
  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;

  // Should find keys using bloom filter
  ASSERT_OK(db_->PrefixExists(ro, "abc1"));
  ASSERT_OK(db_->PrefixExists(ro, "abc2"));

  // Should not find non-existent prefix
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz1").IsNotFound());
}

TEST_F(PrefixExistsTest, NoFilterPolicyInSST) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));

  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(nullptr);  // No filter
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  OpenDB(options);

  // Put and flush to SST
  ASSERT_OK(db_->Put(WriteOptions(), "abc1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "abc2", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;

  // Should still work (fallback to index seek)
  ASSERT_OK(db_->PrefixExists(ro, "abc1"));
  ASSERT_OK(db_->PrefixExists(ro, "abc2"));
  ASSERT_TRUE(db_->PrefixExists(ro, "xyz1").IsNotFound());
}

TEST_F(PrefixExistsTest, KeyInMultipleLevels) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.level0_file_num_compaction_trigger = 2;
  OpenDB(options);

  // Create multiple SST files at different levels
  for (int level = 0; level < 3; ++level) {
    for (int i = 0; i < 100; ++i) {
      std::string key = std::string(1, 'a' + level) + std::string(1, 'a' + i % 26) + std::to_string(i);
      ASSERT_OK(db_->Put(WriteOptions(), key, "value"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  ReadOptions ro;

  // Should find keys at all levels
  ASSERT_OK(db_->PrefixExists(ro, "aa0"));
  ASSERT_OK(db_->PrefixExists(ro, "ba0"));
  ASSERT_OK(db_->PrefixExists(ro, "ca0"));
}

TEST_F(PrefixExistsTest, DeletedKeyInMemtableWithBloom) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  // Put and delete key
  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));

  ReadOptions ro;

  // Should return NotFound (deletion marker found)
  ASSERT_TRUE(db_->PrefixExists(ro, "aa1").IsNotFound());
}

TEST_F(PrefixExistsTest, DeletedKeyInSST) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  OpenDB(options);

  // Put and delete key
  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Delete(WriteOptions(), "aa1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;

  // Should return NotFound (deletion marker in newer SST)
  ASSERT_TRUE(db_->PrefixExists(ro, "aa1").IsNotFound());
}

TEST_F(PrefixExistsTest, SnapshotWithMemtablePrefixBloom) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "aa1", "value1"));

  const Snapshot* snapshot = db_->GetSnapshot();

  // Add more data after snapshot
  ASSERT_OK(db_->Put(WriteOptions(), "aa2", "value2"));

  ReadOptions ro;
  ro.snapshot = snapshot;

  // Should only see data from snapshot
  ASSERT_OK(db_->PrefixExists(ro, "aa1"));
  ASSERT_TRUE(db_->PrefixExists(ro, "aa2").IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(PrefixExistsTest, VeryLargePrefix) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(100));
  OpenDB(options);

  std::string large_key(200, 'a');
  ASSERT_OK(db_->Put(WriteOptions(), large_key, "value"));

  ReadOptions ro;
  ASSERT_OK(db_->PrefixExists(ro, large_key));
}

TEST_F(PrefixExistsTest, SingleCharacterPrefix) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  OpenDB(options);

  ASSERT_OK(db_->Put(WriteOptions(), "a1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "a2", "value2"));
  ASSERT_OK(db_->Put(WriteOptions(), "b1", "value3"));

  ReadOptions ro;

  ASSERT_OK(db_->PrefixExists(ro, "a1"));
  ASSERT_OK(db_->PrefixExists(ro, "b1"));
  ASSERT_TRUE(db_->PrefixExists(ro, "c1").IsNotFound());
}

TEST_F(PrefixExistsTest, EmptyKeyAfterExtraction) {
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(10));
  OpenDB(options);

  // Key shorter than prefix length
  ASSERT_OK(db_->Put(WriteOptions(), "abc", "value"));

  ReadOptions ro;

  // Should handle gracefully
  Status s = db_->PrefixExists(ro, "abc");
  ASSERT_TRUE(s.ok() || s.IsNotFound());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
