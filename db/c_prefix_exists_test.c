/*  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rocksdb/c.h"

int main() {
  rocksdb_t* db;
  rocksdb_options_t* options;
  rocksdb_readoptions_t* roptions;
  rocksdb_writeoptions_t* woptions;
  rocksdb_slicetransform_t* prefix_extractor;
  char* err = NULL;
  char* db_path = "/tmp/rocksdb_c_prefix_exists_test";

  // Clean up any previous test database
  rocksdb_destroy_db(options, db_path, &err);
  if (err != NULL) {
    free(err);
    err = NULL;
  }

  // Create options
  options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(options, 1);

  // Create a fixed prefix extractor (3 bytes)
  prefix_extractor = rocksdb_slicetransform_create_fixed_prefix(3);
  rocksdb_options_set_prefix_extractor(options, prefix_extractor);

  // Open database
  db = rocksdb_open(options, db_path, &err);
  if (err != NULL) {
    fprintf(stderr, "Failed to open database: %s\n", err);
    free(err);
    rocksdb_options_destroy(options);
    return 1;
  }

  // Create read/write options
  roptions = rocksdb_readoptions_create();
  woptions = rocksdb_writeoptions_create();

  // Test 1: Basic prefix exists
  printf("Test 1: Basic prefix exists\n");
  rocksdb_put(db, woptions, "abc1", 4, "value1", 6, &err);
  rocksdb_put(db, woptions, "abc2", 4, "value2", 6, &err);
  rocksdb_put(db, woptions, "def1", 4, "value3", 6, &err);

  unsigned char exists = rocksdb_prefix_exists(db, roptions, "abc1", 4, &err);
  if (exists) {
    printf("  ✓ Prefix 'abc1' exists\n");
  } else {
    printf("  ✗ Prefix 'abc1' should exist\n");
  }

  exists = rocksdb_prefix_exists(db, roptions, "xyz1", 4, &err);
  if (!exists && err == NULL) {
    printf("  ✓ Prefix 'xyz1' does not exist\n");
  } else {
    printf("  ✗ Prefix 'xyz1' should not exist\n");
  }

  // Test 2: Prefix exists with column family
  printf("\nTest 2: Prefix exists with column family\n");
  rocksdb_column_family_handle_t* cf_handle =
      rocksdb_get_default_column_family_handle(db);
  exists = rocksdb_prefix_exists_cf(db, roptions, cf_handle, "def1", 4, &err);
  if (exists) {
    printf("  ✓ Prefix 'def1' exists in default column family\n");
  } else {
    printf("  ✗ Prefix 'def1' should exist in default column family\n");
  }

  // Test 3: Flush and check
  printf("\nTest 3: Prefix exists after flush\n");
  rocksdb_flushoptions_t* flush_options = rocksdb_flushoptions_create();
  rocksdb_flush(db, flush_options, &err);
  rocksdb_flushoptions_destroy(flush_options);

  exists = rocksdb_prefix_exists(db, roptions, "abc1", 4, &err);
  if (exists) {
    printf("  ✓ Prefix 'abc1' exists after flush\n");
  } else {
    printf("  ✗ Prefix 'abc1' should exist after flush\n");
  }

  // Test 4: Delete and check
  printf("\nTest 4: Prefix exists after delete\n");
  rocksdb_delete(db, woptions, "abc1", 4, &err);
  exists = rocksdb_prefix_exists(db, roptions, "abc1", 4, &err);
  if (!exists && err == NULL) {
    printf("  ✓ Prefix 'abc1' does not exist after delete\n");
  } else {
    printf("  ✗ Prefix 'abc1' should not exist after delete\n");
  }

  // Test 5: Performance test
  printf("\nTest 5: Performance test\n");
  // Insert many keys
  for (int i = 0; i < 1000; i++) {
    char key[20];
    snprintf(key, sizeof(key), "key%d", i);
    rocksdb_put(db, woptions, key, strlen(key), "value", 5, &err);
  }
  rocksdb_flush(db, rocksdb_flushoptions_create(), &err);

  // Check many prefixes
  int found_count = 0;
  for (int i = 0; i < 100; i++) {
    char key[20];
    snprintf(key, sizeof(key), "key%d", i);
    exists = rocksdb_prefix_exists(db, roptions, key, strlen(key), &err);
    if (exists) {
      found_count++;
    }
  }
  printf("  ✓ Found %d out of 100 prefixes\n", found_count);

  // Cleanup
  rocksdb_readoptions_destroy(roptions);
  rocksdb_writeoptions_destroy(woptions);
  rocksdb_close(db);
  rocksdb_options_destroy(options);

  // Clean up database
  rocksdb_destroy_db(options, db_path, &err);
  if (err != NULL) {
    free(err);
  }

  printf("\nAll tests completed!\n");
  return 0;
}
