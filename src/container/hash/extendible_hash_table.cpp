//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto dir_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  auto dir_node = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  return dir_node;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  auto bucket_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(bucket_page->GetData());
  return bucket_node;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto dir_node = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_node);
  auto bucket_node = FetchBucketPage(bucket_page_id);
  auto has_value = bucket_node->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return has_value;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_node = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_node);
  auto bucket_node = FetchBucketPage(bucket_page_id);
  // Check whether there exists the same key-value pair.
  // std::vector<ValueType> result;
  // if (bucket_node->GetValue(key, comparator_, &result)) {
    // for (auto v = result.begin(); v != result.end(); v++) {
      // if (*v == value) {
        // buffer_pool_manager_->UnpinPage(directory_page_id_, false);
        // buffer_pool_manager_->UnpinPage(bucket_page_id, false);
        // return false;
      // }
    // }
  // }

  // Check whether the bucket is full.
  while (bucket_node->IsFull()) {
    // Increase global depth if necessary.
    auto bucket_idx = KeyToDirectoryIndex(key, dir_node);
    if (dir_node->GetLocalDepth(bucket_idx) == dir_node->GetGlobalDepth()) {
      // Increase global depth and assign dir_index->bucket_page_id mappings
      dir_node->IncrGlobalDepth();
      for (int i = 0; i < (1 << (dir_node->GetGlobalDepth() - 1)); i++) {
        dir_node->SetBucketPageId(i + (1 << (dir_node->GetGlobalDepth() - 1)), dir_node->GetBucketPageId(i));
        dir_node->SetLocalDepth(i + (1 << (dir_node->GetGlobalDepth() - 1)), dir_node->GetLocalDepth(i));
      }
    } 

    // Increase local depth and new a page and redistribute pairs.
    page_id_t new_page_id0;
    page_id_t new_page_id1;
    auto split_page0 = buffer_pool_manager_->NewPage(&new_page_id0);
    auto split_node0 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_page0->GetData());
    auto split_page1 = buffer_pool_manager_->NewPage(&new_page_id1);
    auto split_node1 = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_page1->GetData());
    // Update dir_index -> bucket_page_id mapping
    dir_node->SetBucketPageId(bucket_idx, new_page_id0);
    dir_node->SetBucketPageId(bucket_idx + (1 << (dir_node->GetGlobalDepth() - 1)), new_page_id1);
    // Update local depths
    dir_node->IncrLocalDepth(bucket_idx);
    dir_node->IncrLocalDepth(bucket_idx + (1 << (dir_node->GetGlobalDepth() - 1)));
    // Redistribute pairs.
    for (uint64_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if (bucket_node->IsReadable(i)) {
        auto new_bucket_idx = KeyToDirectoryIndex(bucket_node->KeyAt(i), dir_node);
        if (dir_node->GetBucketPageId(new_bucket_idx) == new_page_id0) {
          split_node0->Insert(bucket_node->KeyAt(i), bucket_node->ValueAt(i), comparator_);
        } else {
          split_node1->Insert(bucket_node->KeyAt(i), bucket_node->ValueAt(i), comparator_);
        }
      }
    }
    // Release pages and delete old page
    buffer_pool_manager_->UnpinPage(new_page_id0, true);
    buffer_pool_manager_->UnpinPage(new_page_id1, true);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_page_id);
    bucket_page_id = KeyToPageId(key, dir_node);
    bucket_node = FetchBucketPage(bucket_page_id);
  }

  // Now bucket_node is not full
  auto success = bucket_node->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return success;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_node = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_node);
  auto bucket_node = FetchBucketPage(bucket_page_id);
  auto success = bucket_node->Remove(key, value, comparator_);

  // Try to merge if empty
  bool delete_page = false;
  if (bucket_node->IsEmpty()) {
    // How to find its split image? By local depth.
    // Left: bucket_idx < 2^(d-1)
    // Right: bucket_idx >= 2^(d-1)
    auto bucket_idx = KeyToDirectoryIndex(key, dir_node);
    if (dir_node->GetLocalDepth(bucket_idx) > 0) {
      auto split_image_idx = dir_node->GetSplitImageIndex(bucket_idx);
      // auto split_image_bucket = FetchBucketPage(dir_node->GetBucketPageId(split_image_idx));

      // If both have the same local depths, then merge.
      // Move bucket to left index and decrease local depth
      if (dir_node->GetLocalDepth(bucket_idx) == dir_node->GetLocalDepth(split_image_idx)) {
        auto left_split_image_idx = bucket_idx;
        auto right_split_image_idx = split_image_idx;
        if (bucket_idx >= (1 << (dir_node->GetLocalDepth(bucket_idx) - 1))) {
          left_split_image_idx = split_image_idx;
          right_split_image_idx = bucket_idx;
        }
        dir_node->SetBucketPageId(left_split_image_idx, dir_node->GetBucketPageId(left_split_image_idx));
        dir_node->DecrLocalDepth(left_split_image_idx);
        dir_node->SetLocalDepth(right_split_image_idx, 0);

        // Delete page
        delete_page = true;
        
        // Check if we can shrink global depth.
        if (dir_node->CanShrink()) {
          dir_node->DecrGlobalDepth();
        }
      }

    }

  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  if (delete_page) {
    buffer_pool_manager_->DeletePage(bucket_page_id);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, delete_page);
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
