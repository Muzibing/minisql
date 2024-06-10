#include "record/schema.h"
#include <cstddef>
#include <cstdint>
#include "common/macros.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t size = 0;
  MACH_WRITE_TO(std::size_t, buf + size, columns_.size());  // column的num
  size += sizeof columns_.size();
  for (auto i : columns_) {  // 每个column
    size += i->SerializeTo(buf + size);
  }
  MACH_WRITE_TO(bool, buf + size, is_manage_);  // 序列化is_manage_
  size += sizeof is_manage_;
  return size;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof columns_.size() + sizeof is_manage_;  // 累加固定大小字段的大小
  for (auto i : columns_) {
    size += i->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if (schema != nullptr) {
    return 0;
  }
  delete schema;  // 释放schema空间
  uint32_t size = 0;
  std::vector<Column *> columns;
  bool is_manage;
  size_t numColumns = MACH_READ_FROM(std::size_t, buf + size);  // 从缓冲区读取页的数量
  size += sizeof numColumns;
  for (int i = 0; i < numColumns; i++) {
    columns.push_back(nullptr);
    size += Column::DeserializeFrom(buf + size, columns[i]);
  }
  is_manage = MACH_READ_FROM(bool, buf + size);
  schema = new Schema(columns, is_manage);  // 返回反序列化成果
  if (schema == nullptr) {
    return 0;
  } else
    return size;
}