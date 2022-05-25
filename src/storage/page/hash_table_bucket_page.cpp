//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  // Assume result is empty
  // Iterate over all pairs
  for (uint64_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(KeyAt(i), key) == 0) {
        result->push_back(array_[i].second);
      }
    }
  }
  return result->size() > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  // Check duplicate key-value pair
  for (uint64_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      // Duplicate key exists
      if (cmp(KeyAt(i), key) == 0 && ValueAt(i) == value) {
        return false;
      }
    }
  }

  // Check fullness
  if (IsFull()) {
    return false;
  }

  // Scan for unoccupied slot and insert
  for (uint64_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      array_[i].first = key;
      array_[i].second = value;
      SetOccupied(i);
      SetReadable(i);
      return true;
    }
  }
  // Will never reach here
  // Should panic
  LOG_ERROR("all bucket slots are occupied");
  exit(-1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  // Why key and value are both given? Remove by key or by value or both?
  // No method of comparing values. So compare key only. value given is useless. 
  // We can use == to compare value according to project guide.
  // Why is there no removeOccupied?
  // Now it exists
  // find bucket_idx
  for (uint64_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(KeyAt(i), key) == 0 && ValueAt(i) == value) {
        RemoveReadable(i);
        // Why don't we remove occupied_?
        // What's the difference between occupied_ and readable_ anyway?
        // RemoveOccupied(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  RemoveReadable(bucket_idx);
  RemoveOccupied(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  // bucket_idx -> which byte, which bit
  uint32_t byte_idx = bucket_idx / 8;
  uint8_t bit_idx = bucket_idx % 8;
  auto bitmap = occupied_[byte_idx];
  return bitmap & (1 << (7 - bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint8_t bit_idx = bucket_idx % 8;
  auto bitmap = occupied_[byte_idx];
  occupied_[byte_idx] = bitmap | (1 << (7 - bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveOccupied(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint8_t bit_idx = bucket_idx % 8;
  auto bitmap = occupied_[byte_idx];
  occupied_[byte_idx] = bitmap & ((1 << 8) - 1 - (1 << (7 - bit_idx)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t byte_idx = bucket_idx / 8;
  uint8_t bit_idx = bucket_idx % 8;
  auto bitmap = readable_[byte_idx];
  return bitmap & (1 << (7 - bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint8_t bit_idx = bucket_idx % 8;
  auto bitmap = readable_[byte_idx];
  readable_[byte_idx] = bitmap | (1 << (7 - bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveReadable(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint8_t bit_idx = bucket_idx % 8;
  auto bitmap = readable_[byte_idx];
  readable_[byte_idx] = bitmap & ((1 << 8) - 1 - (1 << (7 - bit_idx)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  // Check every bit
  // Not all bits are valid. Some bits are irrelevant.
  for (uint32_t i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8; i++) {
    if (static_cast<uint8_t>(occupied_[i]) != (1 << 8) - 1) {
      return false;
    }
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE % 8; i++) {
    if (!IsOccupied(BUCKET_ARRAY_SIZE - (BUCKET_ARRAY_SIZE % 8) + i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  // Scan all bits
  uint32_t count = 0;
  // Not all bits are valid. Some bits are irrelevant.
  for (uint32_t i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8; i++) {
    for (auto j = 0; j < 8; j++) {
      if (readable_[j] & (1 << j)) {
        count++;
      }
    }
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE % 8; i++) {
    if (IsReadable(BUCKET_ARRAY_SIZE - (BUCKET_ARRAY_SIZE % 8) + i)) {
      count++;
    }
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  // Check every bit
  for (const auto &byte : occupied_) {
    if (static_cast<uint8_t>(byte) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
