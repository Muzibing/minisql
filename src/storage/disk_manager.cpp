#include "storage/disk_manager.h"

#include <page/page.h>
#include <sys/stat.h>

#include <cmath>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  uint32_t id;
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->GetMetaData());
  if (meta_page->GetAllocatedPages() == MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;                                   // 如果当前的页数已满，则返回INVALID_PAGE_ID
  for (page_id_t i = 0; i < meta_page->GetExtentNums(); i++)  // 遍历所有分区
  {
    if (meta_page->GetExtentUsedPage(i) != BITMAP_SIZE) {  // 如果该分区的bitmap未满，则分配一个页
      char buf[PAGE_SIZE];
      ReadPhysicalPage(i * (BITMAP_SIZE + 1) + 1, buf);  // 把对应分区的bitmap读取出来
      BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
      if (bitmap->AllocatePage(id)) {
        meta_page->extent_used_page_[i]++;
        meta_page->num_allocated_pages_++;
        WritePhysicalPage(META_PAGE_ID, this->GetMetaData());  // 重新写回meta_page对应的物理页
        WritePhysicalPage(i * (BITMAP_SIZE + 1) + 1, buf);     // 修改后重新写回bitmap对应物理页中
        return page_id_t(i * BITMAP_SIZE + id);
      }
    }
  }
  // 前面所有分区都已经满了，则开一个新的分区
  char buf[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  if (bitmap->AllocatePage(id)) {  // 分配一个页
    meta_page->num_extents_++;
    uint32_t i = meta_page->num_extents_ - 1;  // 现在的分区编号
    meta_page->extent_used_page_[i]++;
    meta_page->num_allocated_pages_++;
    WritePhysicalPage(META_PAGE_ID, this->GetMetaData());  // 重新写回meta_page对应的物理页
    WritePhysicalPage(i * (BITMAP_SIZE + 1) + 1, buf);     // 修改后重新写回bitmap对应物理页中
    return page_id_t(i * BITMAP_SIZE + id);
  }
  return INVALID_PAGE_ID;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (logical_page_id >= MAX_VALID_PAGE_ID)  // 逻辑页号不合法
    return;
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->GetMetaData());
  uint32_t extend_index = logical_page_id / BITMAP_SIZE;  // 获取对应分区
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;   // 获取该逻辑页在当前分区的页偏移
  char buf[PAGE_SIZE];
  ReadPhysicalPage(extend_index * (BITMAP_SIZE + 1) + 1, buf);  // 读取对应分区的bitmap
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  if (bitmap->DeAllocatePage(page_offset)) {
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[extend_index]--;
    WritePhysicalPage(META_PAGE_ID, this->GetMetaData());          // 更新disk头
    WritePhysicalPage(extend_index * (BITMAP_SIZE + 1) + 1, buf);  // 写回物理页面
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if (logical_page_id >= MAX_VALID_PAGE_ID)  // 逻辑页号不合法
    return false;
  uint32_t extend_index = logical_page_id / BITMAP_SIZE;  // 获取对应分区
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;   // 获取该逻辑页在当前分区的页偏移
  char buf[PAGE_SIZE];
  ReadPhysicalPage(extend_index * (BITMAP_SIZE + 1) + 1, buf);  // 把对应分区的bitmap读取出来
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  return bitmap->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  // logical_page_id : 0~N-1, N~2N-1...
  // physical_page_id : 0 [1 2~N+1] [N+2 N+3~...]...
  // 跳过磁盘元数据0和第一个位图页
  return logical_page_id / BITMAP_SIZE + 2 + logical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
