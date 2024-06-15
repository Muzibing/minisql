#include "buffer/clock_replacer.h"
#include "common/config.h"
#include "glog/logging.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
  for (int i = 0; i < num_pages; i++) {
    frames_.emplace_back(make_pair(false, false));
  }
  // 第一个变量表示该frame_id是否被pin
  clock_hand_ = 0;
}

CLOCKReplacer::~CLOCKReplacer() = default;

/**
 * TODO: Student Implement
 */
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  while (1) {
    // LOG(INFO) << "clock_hand:" << clock_hand_;
    if (clock_hand_ >= frames_.size()) {
      clock_hand_ = 0;
    }
    if (frames_[clock_hand_].first && frames_[clock_hand_].second) {  // 第一个变量没有被pin但是时候未到
      frames_[clock_hand_].second = false;
    } else if (frames_[clock_hand_].first && !frames_[clock_hand_].second) {  // 时候已经到了
      frames_[clock_hand_].first = false;                                     // 使用它并且pin住
      *frame_id = clock_hand_;
      return true;
    }
    clock_hand_++;  // 时钟增加
    if (Size() == 0) {
      break;
    }
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  frames_[frame_id].first = false;
  frames_[frame_id].second = false;
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  frames_[frame_id].first = true;
  frames_[frame_id].second = true;
}

/**
 * TODO: Student Implement
 */
size_t CLOCKReplacer::Size() {
  int size = 0;
  for (auto &frame : frames_) {
    if (frame.first) {
      size++;  // 统计多少frame还没有被pin
    }
  }
  return size;
}

void CLOCKReplacer::Reset(frame_id_t frame_id) {
  frames_[frame_id].first = false;
  frames_[frame_id].second = false;
}