#include "record/row.h"
#include <cstdint>

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = 0;
  uint32_t bitmap_size = (fields_.size() + 7) / 8;  // 计算bitmap所需要的字节数并向上取整
  char *bitmap = new char[bitmap_size];
  std::memset(bitmap, 0, bitmap_size);  // 初始化位图
  for (uint32_t i = 0; i < fields_.size(); i++) {
    uint32_t bitmap_off = i % 8;
    if (fields_[i]->IsNull()) {            // 如果该字段是空
      bitmap[i / 8] |= (1 << bitmap_off);  // 位图上将该位标记为1
    } else {
      bitmap[i / 8] &= ~(1 << bitmap_off);  // 如果非空，标记为0
    }
  }
  MACH_WRITE_UINT32(buf + size, bitmap_size);  // 将位图的大小序列化进去
  size += sizeof(uint32_t);
  memcpy(buf + size, bitmap, bitmap_size);  // 再将位图序列化进去
  size += bitmap_size;

  for (auto temp : fields_) {  // 将每个field都序列化进去
    size += temp->SerializeTo(buf + size);
  }

  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t size = 0;
  uint32_t bitmap_size = MACH_READ_UINT32(buf + size);  // 获取位图大小
  size += sizeof(uint32_t);
  std::string bitmap(buf + size, bitmap_size);  // 获取bitmap
  size += bitmap_size;
  // 下面是为Field反序列化做准备，因为field反序列化需要用到它对应column的字段类型，所以先用schema获取column
  std::vector<Column *> temp = schema->GetColumns();
  // 将field的大小和column的大小统一
  fields_.resize(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {  // field反序列化
    size += Field::DeserializeFrom(buf + size, temp[i]->GetType(), &fields_[i], (bitmap[i / 8] >> (i % 8)) & 1);
  }
  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = sizeof(uint32_t);  // bitmap的大小的大小
  size += (fields_.size() + 7) / 8;  // bitmap的大小
  for (auto temp : fields_) {        // 所有field的大小
    size += temp->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  // key_schema是索引属性集
  // schema 是原表属性集
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);  // 返回该元组的索性属性信息
}
