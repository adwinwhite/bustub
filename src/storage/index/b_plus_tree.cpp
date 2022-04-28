//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID; 
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  auto lookup_page = buffer_pool_manager_->FetchPage(root_page_id_);
  lookup_page->RLatch();
  auto lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
  while (!lookup_node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(lookup_node);
    auto next_lookup_page_id = internal_node->Lookup(key, comparator_);
    lookup_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);
    lookup_page = buffer_pool_manager_->FetchPage(next_lookup_page_id);
    lookup_page->RLatch();
    lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
  }

  auto leaf_node = reinterpret_cast<LeafPage *>(lookup_node);
  ValueType value;
  auto found = leaf_node->Lookup(key, &value, comparator_); 
  if (found) {
    result->push_back(value);
  }
  lookup_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);

  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (root_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory when buffer pool manager created a new page");
  }
  // root_page->RLatch();
  root_page->WLatch();
  auto leaf_node = reinterpret_cast<LeafPage *>(root_page->GetData());
  leaf_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf_node->Insert(key, value, comparator_);
  // root_page->RUnlatch();
  root_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto lookup_page = FindLeafPage(key, true);
  auto leaf_node = reinterpret_cast<LeafPage *>(lookup_page->GetData());
  // Check if the key exists
  ValueType lookup_value;
  auto found = leaf_node->Lookup(key, &lookup_value, comparator_);
  if (found) {
    lookup_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);
    return false;
  }

  // If there is enough space
  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
    // lookup_page->WLatch();
    leaf_node->Insert(key, value, comparator_);
    lookup_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), true);
    return true;
  } 
  // The leaf is full. Split
  auto new_leaf_node_and_page = Split(leaf_node);
  auto new_leaf_node = new_leaf_node_and_page.first;
  auto new_key = new_leaf_node->KeyAt(0);
  if (comparator_(key, new_key) == -1) {
    leaf_node->Insert(key, value, comparator_);
  } else {
    new_leaf_node->Insert(key, value, comparator_);
  }

  // Insert new key to parent
  // two leaf_node are both protected by wlatch. 
  // need to be released and unpinned.
  transaction->AddIntoPageSet(lookup_page);
  transaction->AddIntoPageSet(new_leaf_node_and_page.second);
  InsertIntoParent(leaf_node, new_key, new_leaf_node, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
std::pair<N*, Page*> BPLUSTREE_TYPE::Split(N *node) {
  using std::is_same;
  page_id_t new_page_id;
  auto parent_page_id = node->GetParentPageId();
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  new_page->WLatch();
  auto new_node = reinterpret_cast<N *>(new_page->GetData());
  if constexpr (std::is_same_v<N, LeafPage>) {
    new_node->Init(new_page_id, parent_page_id, leaf_max_size_);
    node->MoveHalfTo(new_node);
    node->SetNextPageId(new_node->GetPageId());
  } else if constexpr (std::is_same_v<N, InternalPage>) {
    new_node->Init(new_page_id, parent_page_id, internal_max_size_);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
  }

  return std::make_pair(new_node, new_page);
}

// INDEX_TEMPLATE_ARGUMENTS
// using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
// INDEX_TEMPLATE_ARGUMENTS
// using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
// INDEX_TEMPLATE_ARGUMENTS
// LeafPage *BPLUSTREE_TYPE::Split(LeafPage *node) {
  // using std::is_same;
  // page_id_t new_page_id;
  // auto parent_page_id = node->GetParentPageId();
  // auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  // auto new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  // new_node->Init(new_page_id, parent_page_id, leaf_max_size_);
  // node->MoveHalfTo(new_node);
  // node->SetNextPageId(new_node->GetPageId());
  // return new_node;
// }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // Check whether parent exists
  if (old_node->IsRootPage()) {
    // No parent. New one
    // Get new root's page id here
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (root_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory when buffer pool manager created a new page");
    }
    root_page->WLatch();
    auto root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    // Update parent_id of old_node and new_node
    old_node->SetParentPageId(root_node->GetPageId());
    new_node->SetParentPageId(root_node->GetPageId());
    root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);

    UpdateRootPageId();

    transaction->WUnlatchPage(old_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    transaction->WUnlatchPage(new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  } else {
    // Parent exists. Insert new key into it
    // No need to update parent id. Unpin child pages
    auto old_page_id = old_node->GetPageId();
    auto new_page_id = new_node->GetPageId();
    transaction->WUnlatchPage(old_page_id);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    transaction->WUnlatchPage(new_page_id);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_page->WLatch();
    // Check whether there is enough space
    if (parent_node->GetSize() < parent_node->GetMaxSize()) {
      parent_node->InsertNodeAfter(old_page_id, key, new_page_id);
      parent_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    } else {
      // Parent is full. Split
      auto parent_split_node_and_page = Split(parent_node);
      auto parent_split_node = parent_split_node_and_page.first;
      // Get middle which will be inserted to parent's parent
      auto new_key = parent_split_node->KeyAt(0);
      // Insert key to parent or its sibling
      if (comparator_(key, new_key) == -1) {
        parent_node->InsertNodeAfter(old_page_id, key, new_page_id);
      } else {
        parent_split_node->InsertNodeAfter(old_page_id, key, new_page_id);
      }

      // Insert new key to grandparent
      transaction->AddIntoPageSet(parent_page);
      transaction->AddIntoPageSet(parent_split_node_and_page.second);
      InsertIntoParent(parent_node, new_key, parent_split_node, transaction);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  RemoveFromLeaf(key, transaction);

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromLeaf(const KeyType &key, Transaction *transaction) {
  auto lookup_page = FindLeafPage(key, true);
  auto leaf_node = reinterpret_cast<LeafPage *>(lookup_page->GetData());
  // Try to delete the record
  auto old_size = leaf_node->GetSize();
  auto new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);


  // No such record
  if (new_size == old_size) {
    lookup_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);
    return;
  }

  // Check whether the leaf is root and size becomes 0
  if (new_size == 0) {
    // Unpin and delete the page
    // No need to write since it's going to be deleted?
    lookup_page->WUnlatch();
    transaction->AddIntoDeletedPageSet(lookup_page->GetPageId());
    buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(lookup_page->GetPageId());
    
    // Update root_page_id
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return;
  }

  // Check whether size is too small
  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
    // Try to borrow from sibling or merge
    if (CoalesceOrRedistribute(leaf_node, transaction)) {
      // This node is merged. Delete
      lookup_page->WUnlatch();
      transaction->AddIntoDeletedPageSet(lookup_page->GetPageId());
      buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(lookup_page->GetPageId());
      return;
    }
  }


  lookup_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), true);
  return;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // How to find my sibling? Check parent to find left and right sibling
  // Should I check both siblings or just one?
  // If I have to merge, prefer left or right?
  if (node->IsRootPage()) {
    // Do nothing when the page is root
    return false;
  }   

  // Not root. Thus has parent and maybe siblings
  // Try to borrow item. First from left sibling
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_page->WLatch();
  auto my_index = parent_node->ValueIndex(node->GetPageId());
  if (my_index > 0) {
    // Has left sibling
    auto left_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(my_index - 1));
    auto left_sibling_node = reinterpret_cast<N *>(left_sibling_page->GetData());
    left_sibling_page->WLatch();
    if (left_sibling_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
      // Move left sibling's last item to my front
      if constexpr (std::is_same_v<N, LeafPage>) {
        left_sibling_node->MoveLastToFrontOf(node);
        // if (comparator_(node->KeyAt(0), parent_node->KeyAt(my_index)) == -1) {
        // parent_node->SetKeyAt(my_index, node->KeyAt(0));
        // }
      } else if constexpr (std::is_same_v<N, InternalPage>) {
        left_sibling_node->MoveLastToFrontOf(node, parent_node->KeyAt(my_index), buffer_pool_manager_); 
        // if (comparator_(node->KeyAt(1), parent_node->KeyAt(my_index)) == -1) {
        // parent_node->SetKeyAt(my_index, node->KeyAt(0));
        // }
      }

      // Update parent's middle key 
      parent_node->SetKeyAt(my_index, node->KeyAt(0));

      // Finish redistribution
      left_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      parent_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      return false;
    }
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), false);
  }

  if (my_index < parent_node->GetSize() - 1) {
    // Has right sibling
    auto right_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(my_index + 1));
    auto right_sibling_node = reinterpret_cast<N *>(right_sibling_page->GetData());
    right_sibling_page->WLatch();
    if (right_sibling_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
      // Move right sibling's first item to my back 
      if constexpr (std::is_same_v<N, LeafPage>) {
        right_sibling_node->MoveFirstToEndOf(node);
      } else if constexpr (std::is_same_v<N, InternalPage>) {
        right_sibling_node->MoveFirstToEndOf(node, parent_node->KeyAt(my_index + 1), buffer_pool_manager_); 
      }

      // Update parent's middle key 
      parent_node->SetKeyAt(my_index + 1, node->KeyAt(0));

      // Finish redistribution
      right_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      parent_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      return false;
    }
    right_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), false);
  }

  // Redistribution doesn't work. Merge
  // Try merging to left
  if (my_index > 0) {
    // Has left sibling
    auto left_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(my_index - 1));
    auto left_sibling_node = reinterpret_cast<N *>(left_sibling_page->GetData());
    left_sibling_page->WLatch();

    // Move all my items to left sibling
    // Only append data
    if constexpr (std::is_same_v<N, LeafPage>) {
      node->MoveAllTo(left_sibling_node);
    } else if constexpr (std::is_same_v<N, InternalPage>) {
      node->MoveAllTo(left_sibling_node, parent_node->KeyAt(my_index), buffer_pool_manager_);
    }

    // Delete parent's middle key 
    parent_node->Remove(my_index);

    // Finish merging
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
    parent_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return true;
  }

  // Try merging to right
  if (my_index < parent_node->GetSize() - 1) {
    // Has right sibling
    auto right_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(my_index + 1));
    auto right_sibling_node = reinterpret_cast<N *>(right_sibling_page->GetData());
    right_sibling_page->WLatch();

    // Move all my items to right sibling
    // BUT
    // We move all data of right sibling to me instead
    if constexpr (std::is_same_v<N, LeafPage>) {
      right_sibling_node->MoveAllTo(node);
    } else if constexpr (std::is_same_v<N, InternalPage>) {
      right_sibling_node->MoveAllTo(node, parent_node->KeyAt(my_index + 1), buffer_pool_manager_);
    }

    // Delete parent's middle key 
    parent_node->Remove(my_index + 1);

    // Finish Merging 
    right_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
    parent_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return true;
  }

  // I has no sibling. 
  // I am the last page of parent

  parent_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  // So many corner cases. I hate c++.
  if (IsEmpty()) {
    return End();
  } else {
    // search first leaf page
    // check whether root is leaf
    auto lookup_page = buffer_pool_manager_->FetchPage(root_page_id_);
    // lookup_page->RLatch();
    auto lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
    while (!lookup_node->IsLeafPage()) {
      auto internal_node = reinterpret_cast<InternalPage *>(lookup_node);
      auto next_lookup_page_id = internal_node->ValueAt(0);
      // lookup_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);
      lookup_page = buffer_pool_manager_->FetchPage(next_lookup_page_id);
      // lookup_page->RLatch();
      lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
    }
    auto leaf_node = reinterpret_cast<LeafPage *>(lookup_node);
    return INDEXITERATOR_TYPE(leaf_node, buffer_pool_manager_);
  }

}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // What if such key does not exist? Just return the next one
  if (IsEmpty()) {
    return End();
  } else {
    // search first leaf page
    // check whether root is leaf
    auto lookup_page = buffer_pool_manager_->FetchPage(root_page_id_);
    // lookup_page->RLatch();
    auto lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
    while (!lookup_node->IsLeafPage()) {
      auto internal_node = reinterpret_cast<InternalPage *>(lookup_node);
      auto next_lookup_page_id = internal_node->Lookup(key, comparator_);
      // lookup_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);
      lookup_page = buffer_pool_manager_->FetchPage(next_lookup_page_id);
      // lookup_page->RLatch();
      lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
    }
    auto leaf_node = reinterpret_cast<LeafPage *>(lookup_node);
    auto key_index = leaf_node->KeyIndex(key, comparator_);
    if (key_index > 0 && comparator_(leaf_node->KeyAt(key_index - 1), key) == 0) {
      key_index--;
    }

    return INDEXITERATOR_TYPE(leaf_node, buffer_pool_manager_, key_index);
  }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { 
  return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // return page protected by wlatch
  // Assume the tree is not empty
  // What is the flag 'leftMost'?
  auto lookup_page = buffer_pool_manager_->FetchPage(root_page_id_);
  lookup_page->WLatch();
  auto lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
  while (!lookup_node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(lookup_node);
    auto next_lookup_page_id = internal_node->Lookup(key, comparator_);
    lookup_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(lookup_page->GetPageId(), false);
    lookup_page = buffer_pool_manager_->FetchPage(next_lookup_page_id);
    lookup_page->WLatch();
    lookup_node = reinterpret_cast<BPlusTreePage *>(lookup_page->GetData());
  }
  return lookup_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  header_page->WLatch();
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
