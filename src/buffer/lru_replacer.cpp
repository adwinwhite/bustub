//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
  std::lock_guard<std::mutex> guard(unpinned_list_mutex_);
  if (unpinned_list_.empty()) {
    return false;
  }
  *frame_id = unpinned_list_.front();
  unpinned_list_.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // vec.erase( std::remove( vec.begin(), vec.end(), val ), vec.end() );
  std::lock_guard<std::mutex> guard(unpinned_list_mutex_);
  unpinned_list_.erase(std::remove(unpinned_list_.begin(), unpinned_list_.end(), frame_id), unpinned_list_.end());
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(unpinned_list_mutex_);
  if (std::find(unpinned_list_.begin(), unpinned_list_.end(), frame_id) == unpinned_list_.end()) {
    unpinned_list_.push_back(frame_id);
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(unpinned_list_mutex_);
  return unpinned_list_.size();
}

}  // namespace bustub
