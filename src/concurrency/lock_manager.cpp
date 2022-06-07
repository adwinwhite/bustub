//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::ExistExclusive(const RID &rid) {
  for (const auto &lr : lock_table_[rid].request_queue_) {
    if (lr.lock_mode_ == LockMode::EXCLUSIVE && lr.granted_) {
      return true;
    }
  }
  return false;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // Check whether the txn is growing.
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  
  // Handle different isolation levels seperately.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // Iterate lock request queue to check whether there exists a exclusive lock granted.
    // And kill the txn.
    for (auto &lr : lock_table_[rid].request_queue_) {
      if (lr.lock_mode_ == LockMode::EXCLUSIVE && lr.granted_) {
        lr.granted_ = false;
        TransactionManager::GetTransaction(lr.txn_id_)->SetState(TransactionState::ABORTED);
      }
    }

    // Grant lock.
    auto lr = LockRequest{txn->GetTransactionId(), LockMode::SHARED};
    lr.granted_ = true;
    lock_table_[rid].request_queue_.push_back(lr);
    txn->GetSharedLockSet()->emplace(rid);
    return true;
  }
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // Check whether the txn is growing.
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  
  // Handle different isolation levels seperately.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // Iterate lock request queue to check whether there exists any lock granted.
    // And kill them.
    for (auto &lr : lock_table_[rid].request_queue_) {
      if (lr.granted_) {
        lr.granted_ = false;
        TransactionManager::GetTransaction(lr.txn_id_)->SetState(TransactionState::ABORTED);
      }
    }

    // Grant lock.
    auto lr = LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE};
    lr.granted_ = true;
    lock_table_[rid].request_queue_.push_back(lr);
    txn->GetExclusiveLockSet()->emplace(rid);
    return true;
  }
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // What's the use of upgrading_?
  // Check whether the txn is growing.
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  
  // Handle different isolation levels seperately.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // Iterate lock request queue to check whether there exists other granted locks.
    // And kill them.
    // First erase my own shared lock.
    lock_table_[rid].request_queue_.remove_if([txn](LockRequest lr){return lr.txn_id_ == txn->GetTransactionId();});
    for (auto &lr : lock_table_[rid].request_queue_) {
      if (lr.granted_) {
        lr.granted_ = false;
        TransactionManager::GetTransaction(lr.txn_id_)->SetState(TransactionState::ABORTED);
      }
    }
    // while (lock_table_[rid].request_queue_.size() != 1 || lock_table_[rid].request_queue_.front().txn_id_ != txn->GetTransactionId()) {
      // // Wait if exists.
      // std::unique_lock<std::mutex> lk(latch_);
      // lock_table_[rid].cv_.wait(lk);
      // lk.unlock();
    // }

    // Grant lock.
    auto lr = LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE};
    lr.granted_ = true;
    lock_table_[rid].request_queue_.push_back(lr);
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->emplace(rid);
    return true;
  }
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // Remove from lock request queue and notify one.
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // Handle different isolation levels seperately.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // Iterate lock request queue to find my lock.
    lock_table_[rid].request_queue_.remove_if([txn](LockRequest lr){return lr.txn_id_ == txn->GetTransactionId();});

    // Notify all.
    lock_table_[rid].cv_.notify_all();

    // Remove lock in txn.
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->erase(rid);
    return true;
  }
  return true;
}

}  // namespace bustub
