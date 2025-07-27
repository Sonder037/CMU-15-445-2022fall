//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // 先从历史链表找
  auto current = list_->GetHistoryList();
  while (current != list_->GetHistoryListEnd()) {
    if (current->evictable_) {
      *frame_id = current->frame_id_;
      list_->Remove(current);
      frame_map_.erase(current->frame_id_);
      return true;
    }
    current = current->next_;
  }

  // 如果历史链表没有可驱逐的节点，则从缓存链表找
  current = list_->GetCacheList();
  while (current != list_->GetCacheListEnd()) {
    if (current->evictable_) {
      *frame_id = current->frame_id_;
      list_->Remove(current);
      frame_map_.erase(current->frame_id_);
      return true;
    }
    current = current->next_;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = frame_map_.find(frame_id);
  if (it != frame_map_.end()) {
    auto node = it->second;
    node->count_++;
    list_->Remove(node);
    if (node->count_ >= k_) {
      // 如果访问次数 >= k，则将节点从历史链表移到缓存链表
      list_->InsertCacheList(node);
    } else {
      // 否则，保持在历史链表中
      list_->InsertHistoryList(node);
    }
    return;
  }
  // 如果是新访问的节点
  auto node = std::make_shared<Node>(frame_id);
  frame_map_[frame_id] = node;
  if (node->count_ >= k_) {
    list_->InsertCacheList(node);
  } else {
    list_->InsertHistoryList(node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    throw std::runtime_error("SetEvictable: Frame not found in replacer");
    return;
  }
  auto node = it->second;
  if (!node->evictable_ && set_evictable) {
    node->evictable_ = true;
    list_->IncrementSize();
  }
  if (node->evictable_ && !set_evictable) {
    node->evictable_ = false;
    list_->DecrementSize();
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    return;
  }
  auto node = it->second;
  if (!node->evictable_) {
    throw std::exception();
    return;
  }
  list_->Remove(node);
  frame_map_.erase(it);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return list_->Size();
}

}  // namespace bustub
