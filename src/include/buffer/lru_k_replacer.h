//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation #finished
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  class Node;
  class List;

  /**
   * @brief : 查找结点
   */
  auto Find(frame_id_t frame_id) -> std::shared_ptr<Node> {
    auto it = frame_map_.find(frame_id);
    if (it != frame_map_.end()) {
      return it->second;
    }
    return nullptr;
  }
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.

  [[maybe_unused]] size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
  std::shared_ptr<List> list_{std::make_shared<List>()};
  std::unordered_map<frame_id_t, std::shared_ptr<Node>> frame_map_{};

  class Node {
   public:
    Node() = default;
    explicit Node(frame_id_t frame_id) : frame_id_(frame_id), count_(1) {}

    frame_id_t frame_id_{-1};
    size_t count_{0};
    bool evictable_{false};
    std::shared_ptr<Node> prev_;
    std::shared_ptr<Node> next_;
  };

  class List {
   public:
    List() : head_(std::make_shared<Node>()), middle_(std::make_shared<Node>()), tail_(std::make_shared<Node>()) {
      head_->next_ = middle_;
      middle_->prev_ = head_;
      middle_->next_ = tail_;
      tail_->prev_ = middle_;
    }

    ~List() {
      auto current = middle_->next_;
      while (current != tail_) {
        auto next = current->next_;
        current->prev_.reset();
        current->next_.reset();
        current = next;
      }

      current = head_->next_;
      while (current != middle_) {
        auto next = current->next_;
        current->prev_.reset();
        current->next_.reset();
        current = next;
      }

      head_->next_.reset();
      middle_->prev_.reset();
      middle_->next_.reset();
      tail_->prev_.reset();
    }

    auto Size() -> size_t { return size_; }

    auto IncrementSize() -> void { size_++; }

    auto DecrementSize() -> void {
      if (size_ > 0) {
        size_--;
      }
    }

    auto Remove(std::shared_ptr<Node> &node) -> void {
      node->prev_->next_ = node->next_;
      node->next_->prev_ = node->prev_;
      node->next_.reset();
      node->prev_.reset();
      if (node->evictable_) {
        size_--;
      }
    }

    /**
     * @brief 加入到 cache list ，即访问次数 >=k 次的节点
     */
    auto InsertCacheList(std::shared_ptr<Node> &node) -> void {
      node->prev_ = middle_->prev_;
      node->next_ = middle_;
      middle_->prev_->next_ = node;
      middle_->prev_ = node;
      if (node->evictable_) {
        size_++;
      }
    }

    /**
     * @brief 加入到 history list ，即访问次数 <k 次的节点
     */
    auto InsertHistoryList(std::shared_ptr<Node> &node) -> void {
      node->next_ = tail_;
      node->prev_ = tail_->prev_;
      tail_->prev_->next_ = node;
      tail_->prev_ = node;
      if (node->evictable_) {
        size_++;
      }
    }

    auto GetCacheList() -> std::shared_ptr<Node> { return head_->next_; }

    auto GetHistoryList() -> std::shared_ptr<Node> { return middle_->next_; }

    auto GetCacheListEnd() -> std::shared_ptr<Node> { return middle_; }

    auto GetHistoryListEnd() -> std::shared_ptr<Node> { return tail_; }

   private:
    size_t size_{0};
    std::shared_ptr<Node> head_;
    std::shared_ptr<Node> middle_;
    std::shared_ptr<Node> tail_;
  };
};

}  // namespace bustub
