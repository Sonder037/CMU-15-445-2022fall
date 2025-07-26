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
  std::lock_guard<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  if (index >= dir_.size()) {
    return false;
  }
  const auto &bucket = dir_[index];
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  if (index > dir_.size()) {
    return false;
  }
  const auto &bucket = dir_[index];
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  while (!dir_[index]->Insert(key, value)) {
    if (dir_[index]->GetDepth() == global_depth_) {
      ExpandTable();
      // 扩展完重新计算 index
      index = IndexOf(key);
    }
    // 当前bucket已满，分裂bucket
    SplitBucket(dir_[index]);
  }
}

// 扩展table
template <typename K, typename V>
void ExtendibleHashTable<K, V>::ExpandTable() {
  ++global_depth_;
  size_t old_size = dir_.size();
  size_t new_size = old_size << 1;
  dir_.resize(new_size);
  for (size_t i = 0; i < old_size; ++i) {
    dir_[i + old_size] = dir_[i];
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(std::shared_ptr<Bucket> bucket) {
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
  bucket->IncrementDepth();
  auto &items = bucket->GetItems();
  auto it = items.begin();
  size_t index = 0;
  while (it != items.end()) {
    index = IndexOf(it->first);

    if (index & (1 << (bucket->GetDepth() - 1))) {
      new_bucket->Insert(it->first, it->second);
      it = items.erase(it);
    } else {
      ++it;
    }
  }
  num_buckets_++;
  /**
   * @TODO: 只遍历 xxxx+1+mask 对应位置的bucket
   */
  // index = index & ((1 << (bucket->GetDepth() - 1)) - 1);
  // for (size_t i = (1 << (bucket->GetDepth())) + index, step = (1 << (bucket->GetDepth())); i < dir_.size(); i +=
  // step) {
  //   if (dir_[i] == bucket) {
  //     dir_[i] = new_bucket;
  //   }
  // }
  for (size_t i = 0; i < dir_.size(); i++) {
    if (dir_[i] == bucket && (i >> (bucket->GetDepth() - 1)) & 1) {
      dir_[i] = new_bucket;
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&key](const std::pair<K, V> &item) { return item.first == key; });
  if (it != list_.end()) {
    list_.erase(it);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&key](const std::pair<K, V> &item) { return item.first == key; });
  if (it != list_.end()) {
    it->second = value;
    return true;
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
