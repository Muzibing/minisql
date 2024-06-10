#include "record/column.h"
#include <sys/types.h>
#include <cstddef>
#include <cstdint>

#include "common/dberr.h"
#include "common/macros.h"
#include "glog/logging.h"
#include "record/type_id.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
 * TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t size = 0;
  size_t name_size = this->name_.length();  // 获取字段名的长度
  MACH_WRITE_TO(size_t, buf + size, name_size);
  size += sizeof name_size;
  MACH_WRITE_STRING(buf + size, this->name_);  // 将字段名写入buf
  size += name_.length();
  MACH_WRITE_TO(TypeId, buf + size, this->type_);  // 将字段类型写入buf
  size += sizeof type_;
  if (this->type_ == kTypeChar) {                     // 如果是char类型需要写入buf
    MACH_WRITE_TO(uint32_t, buf + size, this->len_);  // 字符类型考虑字符长度
    size += sizeof len_;
  }
  MACH_WRITE_TO(uint32_t, buf + size, this->table_ind_);  // 将字段在表中的索引写入buf
  size += sizeof table_ind_;
  MACH_WRITE_TO(bool, buf + size, this->nullable_);  // 将字段是否可以为空写入buf
  size += sizeof nullable_;
  MACH_WRITE_TO(bool, buf + size, this->unique_);  // 将字段是否唯一写入buf
  size += sizeof unique_;
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  int i = 0;
  if (type_ == kTypeChar) {
    i = sizeof len_;
  }
  return name_.length() + sizeof(size_t) + sizeof type_ + sizeof table_ind_ + sizeof nullable_ + sizeof unique_ + i;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {  // 若column不是空，则不做任何操作
    return 0;
  }
  size_t name_size;
  uint32_t size = 0, len;
  std::string name = "";
  char c;
  name_size = MACH_READ_FROM(size_t, buf + size);  // namesize存储buf中字段名的长度
  size += sizeof name_size;
  for (size_t i = 0; i < name_size; i++) {  // name存储后面的字段名
    c = MACH_READ_FROM(char, buf + size + i * sizeof(char));
    name.push_back(c);
  }
  size += name.length();
  TypeId type = MACH_READ_FROM(TypeId, buf + size);  // type存储字段类型
  size += sizeof type;
  if (type == kTypeChar) {  // 如果字段是char类型，再用len存储规定的data最大长度
    len = MACH_READ_FROM(uint32_t, buf + size);
    size += sizeof len;
  }
  uint32_t table_ind = MACH_READ_FROM(uint32_t, buf + size);  // 存储字段在表中的位置
  size += sizeof table_ind;
  bool nullable = MACH_READ_FROM(bool, buf + size);
  size += sizeof nullable;
  bool unique = MACH_READ_FROM(bool, buf + size);
  size += sizeof unique;
  if (type == kTypeChar) {  // 按type类型选择不同构造函数
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  if (column == nullptr) {  // 判断创建column失败否
    return 0;
  } else
    return size;  // 返回向后反序列化移动的字节
}
