#include "page/bitmap_page.h"

#include "glog/logging.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ == 8 * MAX_CHARS) {
    // LOG(WARNING) << "there is no page available" <<std::endl;
    return false;
  }
  bytes[next_free_page_ >> 3] |= (0x80 >> (next_free_page_ & 0x07));
  // 置位操作：左边是字节 右边是算位数 eg：比如将第1234个位置1 ，字节序：1234 >> 3 = 154; 位序：0x80 >> (1234 & 0x07) =
  // 2 ，那么 1234 放在 M 的下标 154 字节处，把该字节的 2 号位（ 0~7）置为 1
  page_allocated_++;
  page_offset = next_free_page_;
  next_free_page_ = 8 * MAX_CHARS;
  for (uint32_t i = (page_offset + 1); i < 8 * MAX_CHARS; i++) {
    if ((bytes[i >> 3] & (0x80 >> (i & 0x07))) == 0) {
      next_free_page_ = i;
      // 当前是空闲页
      break;
    }
  }
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFree(page_offset)) {
    // LOG(WARNING) << "this page is already available" <<std::endl;
    return false;
  }
  bytes[page_offset >> 3] &= ~(0x80 >> (page_offset & 0x07));
  // 复位操作
  page_allocated_--;
  if (next_free_page_ > page_offset) next_free_page_ = page_offset;
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (bytes[byte_index] & (1 << (7 - bit_index))) return false;
  return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;