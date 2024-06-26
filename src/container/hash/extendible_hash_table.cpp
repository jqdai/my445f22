//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  auto index = IndexOf(key);
  if (index >= dir_.size()) {
    latch_.unlock();
    return false;
  }
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  auto index = IndexOf(key);
  if (index >= dir_.size()) {
    return false;
  }
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  size_t index = IndexOf(key);
  if (index >= dir_.size()) {
    return;
  }
  if (dir_[index]->Insert(key, value)) {
    return;
  }
  std::shared_ptr<Bucket> oldBucket = dir_[index];
  if (dir_[index]->GetDepth() == global_depth_) {
    oldBucket->IncrementDepth();
    size_t oldDirSize = dir_.size();
    int OldMask = (1 << global_depth_) - 1;
    global_depth_ ++;
    int newMask = (1 << global_depth_) - 1;
    std::shared_ptr<Bucket> newBucket = std::make_shared<Bucket>(bucket_size_, global_depth_);
    std::list<std::pair<K, V>> oldBucketItems = oldBucket->GetItems();
    num_buckets_ ++;
    size_t newDirSize = 1 << global_depth_;

    for (size_t i = oldDirSize; i < newDirSize; i ++) {
      if ((i & newMask) != index && (i & OldMask) == index) {
        for (auto kvp : oldBucketItems) {
          if (IndexOf(kvp.first) == i) {
            newBucket->Insert(kvp.first, kvp.second);
            oldBucket->Remove(kvp.first);
          }
        }
        dir_.emplace_back(newBucket);
      } else {
        dir_.emplace_back(nullptr);
      }
    }
    dir_[IndexOf(key)]->Insert(key, value);
    for (size_t i = oldDirSize; i < newDirSize; i ++) {
      if (dir_[i] == nullptr) {
        dir_[i] = dir_[i & OldMask];
      }
    }
  } else {
    std::list<std::pair<K, V>> oldBucketList = dir_[index]->GetItems();
    int newLocalDepth = oldBucket->GetDepth() + 1;
    oldBucket->IncrementDepth();
    std::shared_ptr<Bucket> newBucket = std::make_shared<Bucket>(bucket_size_, newLocalDepth);
    int newMask = (1 << newLocalDepth) - 1;
    auto sampleHash = std::hash<K>()(oldBucketList.front().first) & newMask;
    for (auto kvp : oldBucketList) {
      if ((std::hash<K>()(kvp.first) & newMask) != sampleHash) {
        newBucket->Insert(kvp.first, kvp.second);
        oldBucket->Remove(kvp.first);
      }
    }
    for (size_t i = 0; i < dir_.size(); i ++) {
      if (dir_[i] == oldBucket && ((i & newMask) != sampleHash)) {
        dir_[i] = newBucket;
      }
    }
    num_buckets_ ++;
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); it ++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); it ++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  if (IsFull()) {
    return false;
  }
  for (auto it = list_.begin(); it != list_.end(); it ++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
