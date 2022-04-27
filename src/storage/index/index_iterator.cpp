/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_page, BufferPoolManager *buf, int key_index)
    : key_index(key_index), leaf_page(leaf_page), buf(buf) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page == nullptr) {
    return;
  }
  if (leaf_page->GetPageId() != INVALID_PAGE_ID) {
    buf->UnpinPage(leaf_page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() {
  if (leaf_page == nullptr) {
    return true;
  }
  if (leaf_page->GetPageId() == INVALID_PAGE_ID) {
    return true;
  }
  if (leaf_page->GetNextPageId() == INVALID_PAGE_ID && key_index == leaf_page->GetSize() - 1) {
    return true;
  } else {
    return false;
  }
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
  // Stupid c++, stupid me
  return leaf_page->GetItem(key_index); 
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (leaf_page == nullptr) {
    return *this;
  }
  key_index++;
  if (key_index >= leaf_page->GetSize()) {
    if (leaf_page->GetNextPageId() == INVALID_PAGE_ID) {
      // Reach end
      leaf_page = nullptr;
      key_index = 0;
    } else {
      auto lookup_page = buf->FetchPage(leaf_page->GetNextPageId());
      // lookup_page->RLatch();
      auto lookup_node =
          reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(lookup_page->GetData());
      buf->UnpinPage(leaf_page->GetPageId(), false);
      leaf_page = lookup_node;
      key_index = 0;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
