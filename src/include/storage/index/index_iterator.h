//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_page, BufferPoolManager *buf, int key_index=0);
  ~IndexIterator();

  bool IsEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    if (leaf_page == nullptr && itr.leaf_page == nullptr) {
      return true;
    } else if (leaf_page == nullptr && itr.leaf_page != nullptr) {
      return false;
    } else if (leaf_page != nullptr && itr.leaf_page == nullptr) {
      return false;
    } 
    if (key_index == itr.key_index && leaf_page->GetPageId() == itr.leaf_page->GetPageId()) {
      return true;
    } else {
      return false;
    }
  }

  bool operator!=(const IndexIterator &itr) const {
    return !operator==(itr);
  }

 private:
  // add your own private member variables here
  int key_index;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page;
  BufferPoolManager *buf;
};

}  // namespace bustub
