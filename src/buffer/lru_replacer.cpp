#include "buffer/lru_replacer.h"
#include "common/config.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  // LRUReplacer中记录每一个页的访问次数
  // 页帧下标从0开始
  frame_used.resize(num_pages);
  frame_isPin.resize(num_pages);
  for (int i = 0; i < num_pages; i++) {
    frame_used[i] = -1;     // 表示这个页是空的
    frame_isPin[i] = true;  // 空页类似于Pin，不能被替换
  }
  number_unpined_frame = 0;   // 全是空页
  replacer_size = num_pages;  // 最大页数
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  *frame_id = INVALID_PAGE_ID;
  uint32_t minTime = UINT32_MAX;

  for (int i = 0; i < replacer_size; i++)
    // 能够替换的条件：不是空页，UnPin，访问次数更少
    if (frame_used[i] != -1 && !frame_isPin[i] && frame_used[i] < minTime) {
      minTime = frame_used[i];
      *frame_id = i;
    }
  if (*frame_id != INVALID_PAGE_ID)  // 存在这样的可替换页
  {
    frame_used[*frame_id] = -1;  // 删除后为空页
    frame_isPin[*frame_id] = true;
    number_unpined_frame--;
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  // 如果Pin的页不是空页再操作(空页不能Pin)
  if (frame_used[frame_id] != -1) {
    if (!frame_isPin[frame_id]) number_unpined_frame--;  // 原先是UnPin的变为Pin，可替换页数减一
    frame_isPin[frame_id] = true;                        // 标记Pin
    frame_used[frame_id]++;                              // 访问次数++
  }
  return;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_used[frame_id] == -1)  // 空页Unpin相当于向缓冲区添加一个新页
  {
    frame_used[frame_id] = 0;
    frame_isPin[frame_id] = false;
    number_unpined_frame++;
    return;
  }
  if (frame_isPin[frame_id])  // 不是空页，原先是Pin的页
  {
    frame_isPin[frame_id] = false;  // 解除Pin
    number_unpined_frame++;
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() { return number_unpined_frame; }

void LRUReplacer::Reset(frame_id_t frame_id) {
  frame_used[frame_id] = -1;
  frame_isPin[frame_id] = true;
}
