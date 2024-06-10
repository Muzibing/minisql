#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

  void Reset(frame_id_t frame_id) override;

 private:
  // add your own private member variables here
  vector<int32_t> frame_used;   // 记录缓冲区每一页帧的访问次数
  vector<bool> frame_isPin;     // 记录缓冲区每一页是否被pin
  size_t number_unpined_frame;  // 记录缓冲区有多少页可以替换
  size_t replacer_size;         // 缓冲区最大容量
};

#endif  // MINISQL_LRU_REPLACER_H
