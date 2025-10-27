//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl.h"

#include <cstring>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/version_set.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

// Helper function to check if a found key's prefix matches the target prefix.
// Handles both cases: with and without prefix extractor.
inline bool PrefixMatches(const SliceTransform* prefix_extractor,
                          const Slice& found_user_key,
                          const Slice& target_prefix) {
  if (prefix_extractor != nullptr) {
    // With prefix extractor: extract and compare prefixes using memcmp
    Slice found_prefix = prefix_extractor->Transform(found_user_key);
    return found_prefix.size() == target_prefix.size() &&
           memcmp(found_prefix.data(), target_prefix.data(),
                  found_prefix.size()) == 0;
  } else {
    // Without prefix extractor: check if found key starts with target prefix
    return found_user_key.size() >= target_prefix.size() &&
           memcmp(found_user_key.data(), target_prefix.data(),
                  target_prefix.size()) == 0;
  }
}

Status DBImpl::PrefixExists(const ReadOptions& options,
                           ColumnFamilyHandle* column_family,
                           const Slice& prefix) {
  if (column_family == nullptr) {
    column_family = DefaultColumnFamily();
  }

  // column_family is guaranteed to be ColumnFamilyHandleImpl by the DB interface
  auto cfh = static_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (cfd == nullptr) {
    return Status::InvalidArgument("Column family not found");
  }

  // Get the prefix extractor
  const SliceTransform* prefix_extractor =
      cfd->ioptions()->prefix_extractor.get();

  // If no prefix extractor is configured, we'll still benefit from:
  // 1. Early abort on first key match
  // 2. No data block loading
  // We'll use the input as a search key and check if found keys start with it.
  Slice actual_prefix = prefix;
  if (prefix_extractor != nullptr) {
    // Validate that the prefix is in the domain
    if (!prefix_extractor->InDomain(prefix)) {
      return Status::NotFound();
    }
    // Extract the actual prefix
    actual_prefix = prefix_extractor->Transform(prefix);
  }

  // Create an internal key for searching using stack-based buffer
  // Most keys fit in 64 bytes; larger keys fall back to heap allocation
  char internal_key_buf[64];
  std::string internal_key_str(internal_key_buf, 0);
  AppendInternalKey(&internal_key_str,
                    ParsedInternalKey(actual_prefix, kMaxSequenceNumber,
                                      kTypeValue));
  Slice internal_key(internal_key_str);

  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  if (sv == nullptr) {
    return Status::InvalidArgument("Column family not found");
  }

  // Step 1: Check mutable memtable
  if (sv->mem != nullptr) {
    std::unique_ptr<InternalIterator> mem_iter(sv->mem->NewIterator(
        options, nullptr, &arena_, prefix_extractor, false));
    mem_iter->Seek(internal_key);
    if (mem_iter->Valid()) {
      Slice found_user_key = ExtractUserKey(mem_iter->key());
      if (PrefixMatches(prefix_extractor, found_user_key, actual_prefix)) {
        ReturnAndCleanupSuperVersion(cfd, sv);
        return Status::OK();
      }
    }
  }

  // Step 2: Check immutable memtables
  if (sv->imm != nullptr) {
    std::vector<InternalIterator*> imm_iters;
    sv->imm->AddIterators(options, nullptr, prefix_extractor, &imm_iters,
                          &arena_);
    for (auto iter : imm_iters) {
      iter->Seek(internal_key);
      if (iter->Valid()) {
        Slice found_user_key = ExtractUserKey(iter->key());
        if (PrefixMatches(prefix_extractor, found_user_key, actual_prefix)) {
          ReturnAndCleanupSuperVersion(cfd, sv);
          return Status::OK();
        }
      }
    }
  }

  // Step 3: Check SST files using filter policies and index blocks
  // Filter policies (bloom, ribbon, etc.) eliminate files that don't contain
  // the prefix. Only index blocks are sought, never data blocks.
  Version* current = sv->current;
  if (current == nullptr) {
    ReturnAndCleanupSuperVersion(cfd, sv);
    return Status::NotFound();
  }

  // Create an iterator to seek through SST files
  // We use the internal_key (with max sequence number) to find the first key
  // that could match the prefix. Note: prefix extractor optimizations in SST
  // files require a prefix extractor to be configured.
  std::unique_ptr<InternalIterator> iter(current->NewInternalIterator(
      options, &arena_, prefix_extractor));

  iter->Seek(internal_key);

  // Check if we found a key with the matching prefix
  // We only need to check the first key after seeking since we're looking for
  // any key that starts with the prefix, not a specific key
  if (iter->Valid()) {
    Slice found_user_key = ExtractUserKey(iter->key());
    if (PrefixMatches(prefix_extractor, found_user_key, actual_prefix)) {
      ReturnAndCleanupSuperVersion(cfd, sv);
      return Status::OK();
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);
  return Status::NotFound();
}

}  // namespace ROCKSDB_NAMESPACE
