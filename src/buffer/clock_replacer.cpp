#include "buffer/clock_replacer.h"
#include "common/config.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
  for (size_t i = 0; i < num_pages; ++i) {
    frames_.push_back(std::make_tuple(false, false));
  }
}

CLOCKReplacer::~CLOCKReplacer() = default;

/**
 * TODO: Student Implement
 */
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (Size() == 0) {
    return false;
  }

  std::lock_guard<std::shared_mutex> lock(mutex_);
  while (true) {
    auto &[contains, ref] = frames_[clock_hand_];
    if (contains) {
      if (ref) {
        ref = false;
      } else {
        *frame_id = clock_hand_;
        contains = false;
        return true;
      }
    }
    clock_hand_ = (clock_hand_ + 1) % frames_.size();
  }
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (frame_id < frames_.size()) return;
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = false;
  ref = false;
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id < frames_.size()) return;
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = true;
  ref = true;
}

/**
 * TODO: Student Implement
 */
size_t CLOCKReplacer::Size() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  size_t size = 0;
  for (auto &[contains, ref] : frames_) {
    size += contains;
  }
  return size;
}

void CLOCKReplacer::Reset(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = false;
  ref = false;
}