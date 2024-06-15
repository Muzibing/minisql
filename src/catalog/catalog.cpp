#include "catalog/catalog.h"
#include <asm-generic/errno.h>
#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>
#include "catalog/indexes.h"
#include "catalog/table.h"
#include "common/config.h"
#include "common/dberr.h"
#include "glog/logging.h"
#include "record/field.h"
#include "storage/table_heap.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  LOG(INFO) << magic_num;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(decltype(index_meta_pages_.size())) * 2 +
          table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
          index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = 0;
    next_index_id_ = 0;
  } else {
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(
        buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID)->GetData()));  // 获取catalogmeta的信息
    next_table_id_ = catalog_meta_->GetNextTableId();                       // 获取下一个tableid
    next_index_id_ = catalog_meta_->GetNextIndexId();                       // 获取下一个indexid
    for (auto iter : catalog_meta_->table_meta_pages_) {
      // 获取所有元信息的目录
      auto table_meta_page = buffer_pool_manager->FetchPage(iter.second);
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);  // 反序列化表的元信息
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();     // 获取表名
      auto table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                          log_manager_, lock_manager_);
      TableInfo *table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);        // 初始化table_info
      tables_[table_meta->GetTableId()] = table_info;  // 赋值
      if (table_meta->GetTableId() >= next_table_id_) {
        next_table_id_ = table_meta->GetTableId() + 1;  // 更新next
      }
    }
    for (auto iter : catalog_meta_->index_meta_pages_) {
      auto index_meta_page = buffer_pool_manager->FetchPage(iter.second);  // 获取该index元信息所在的页
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);  // 反序列化index的元信息
      index_names_[tables_[index_meta->GetTableId()]->GetTableName()][index_meta->GetIndexName()] =
          index_meta->GetIndexId();  // index_names_[表名][索引名]=index_id
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_[index_meta->GetIndexId()] = index_info;
      if (index_meta->GetIndexId() >= next_index_id_) {
        next_index_id_ = index_meta->GetIndexId() + 1;
      }
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  auto temp = table_names_.find(table_name);
  if (temp != table_names_.end()) {  // 表名已经存在
    // table_info = tables_[temp->second];
    return DB_ALREADY_EXIST;
  }
  table_info = TableInfo::Create();
  next_table_id_ = catalog_meta_->GetNextTableId();
  table_names_[table_name] = next_table_id_;
  page_id_t id;
  auto page = buffer_pool_manager_->NewPage(id);
  auto copy_schema = Schema::DeepCopySchema(schema);  // 深拷贝，如果schema在函数执行期间被修改，不会影响到正在创建的表
  TableHeap *heap_ = TableHeap::Create(buffer_pool_manager_, copy_schema, txn, log_manager_,
                                       lock_manager_);  // 创建堆表和表的元信息并序列化到page
  TableMetadata *table_meta_ = TableMetadata::Create(next_table_id_, table_name, heap_->GetFirstPageId(), copy_schema);
  table_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(id, true);
  table_info->Init(table_meta_, heap_);  // 创建table信息并存储到tables_中
  catalog_meta_->table_meta_pages_[next_table_id_] = id;
  tables_[next_table_id_] = table_info;

  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)
                                 ->GetData());  // 将catalog_meta序列化到page[CATALOG_META_PAGE_ID]中
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.count(table_name) <= 0) {  // 没找到
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];  // 根据name找到id
  table_info = tables_[table_id];                  // 根据id找到info
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.size() == 0) {
    return DB_FAILED;
  }
  tables.resize(tables_.size());
  uint32_t i = 0;
  for (auto iter = tables_.begin(); iter != tables_.end(); iter++, i++) {
    tables[i] = iter->second;  // table_中值存的是info
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  auto iter_find_table = table_names_.find(table_name);  // table要存在的
  if (iter_find_table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto iter_find_index_table = index_names_.find(table_name);  // index要是不存在的
  if (iter_find_index_table != index_names_.end()) {
    auto iter_find_index_name = iter_find_index_table->second.find(index_name);
    if (iter_find_index_name != iter_find_index_table->second.end()) {
      return DB_INDEX_ALREADY_EXIST;
    }
  }

  table_id_t table_id = 0;
  TableSchema *schema_ = nullptr;
  TableInfo *table_info = nullptr;
  page_id_t meta_page_id = 0;
  Page *meta_page = nullptr;
  index_id_t index_id = 0;
  IndexMetadata *index_meta_ = nullptr;
  uint32_t column_index = 0;

  std::vector<uint32_t> key_map{};
  index_info = IndexInfo::Create();

  table_id = table_names_[table_name];  // 获取table schema
  table_info = tables_[table_id];
  schema_ = table_info->GetSchema();
  for (auto column_name : index_keys) {  // 获取该行index并存储到keymap
    if (schema_->GetColumnIndex(column_name, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(column_index);
  }
  meta_page = buffer_pool_manager_->NewPage(meta_page_id);  // 获取一个新页来存储索引元信息
  index_id = catalog_meta_->GetNextIndexId();
  index_meta_ = IndexMetadata::Create(index_id, index_name, table_id, key_map);  // 创建索引元信息
  index_meta_->SerializeTo(meta_page->GetData());                                // 索引元信息序列化
  index_info->Init(index_meta_, table_info, buffer_pool_manager_);
  index_names_[table_name][index_name] = index_id;  // 存储indexinfo
  indexes_[index_id] = index_info;

  // 创建索引时遍历tableheap中所有元素
  auto table_heap = table_info->GetTableHeap();
  vector<Field> f;
  for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); iter++) {
    f.clear();
    for (auto pos : key_map) {  // 对于堆表中每一条记录获取对应搜索键的每一个元组
      f.push_back(*(iter->GetField(pos)));
    }
    Row row(f);  // 利用这些筛选出来的元组组成一个新row
    index_info->GetIndex()->InsertEntry(row, iter->GetRowId(), nullptr);
  }
  catalog_meta_->index_meta_pages_[index_id] = meta_page_id;  // 存储meta_page的id
  Page *page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char *buf = page_->GetData();  // 序列化到page中
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;  // 确保table存在
  }
  auto index_nametoid = index_names_.find(table_name)->second;    // 获取index_name to id的映射
  if (index_nametoid.find(index_name) == index_nametoid.end()) {  // 如果不存在
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = index_nametoid[index_name];  // 获取id和info
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto table_indexes = index_names_.find(table_name);
  if (table_indexes == index_names_.end()) {  // 确保table存在
    return DB_TABLE_NOT_EXIST;
  }
  auto indexes_map = table_indexes->second;
  for (auto iter : indexes_map) {  // 将所有index_info存储到indexes中
    indexes.push_back(indexes_.find(iter.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;  // 确保table存在
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];
  if (table_info == nullptr) {
    return DB_FAILED;
  }
  tables_.erase(table_id);         // 清除id对应的映射
  table_names_.erase(table_name);  // 清除num对应的映射
  table_info->~TableInfo();        // 析构table对应的tableInfo
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto table_indexes = index_names_.find(table_name);
  if (table_indexes == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;  // 确保table存在
  }
  auto index_tabletonametoid = index_names_.find(table_name);
  if (index_tabletonametoid == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;  // 确保index存在
  }
  auto index_nametoid = index_tabletonametoid->second.find(index_name);  // 根据index_name找到index_name和index_id
  if (index_nametoid == index_tabletonametoid->second.end()) {
    return DB_INDEX_NOT_FOUND;
  } else {
    delete indexes_[index_nametoid->second];          // index_id对应的info删掉
    index_tabletonametoid->second.erase(index_name);  // 删除映射关系name,id
    indexes_.erase(index_nametoid->second);           // 删除映射关系id,info
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto CatalogMetaPage = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(reinterpret_cast<char *>(CatalogMetaPage->GetData()));
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *meta_page = nullptr;
  page_id_t table_page_id = 0;
  string table_name = "";
  TableMetadata *table_meta = nullptr;
  TableHeap *table_heap = nullptr;
  TableSchema *schema = nullptr;
  TableInfo *table_info = nullptr;
  table_info = table_info->Create();                              // 先初始化table_info
  meta_page = buffer_pool_manager_->FetchPage(page_id);           // 获取table_meta_page
  table_meta->DeserializeFrom(meta_page->GetData(), table_meta);  // data反序列化到table_meta中
  table_name = table_meta->GetTableName();
  schema = table_meta->GetSchema();
  table_page_id = table_meta->GetFirstPageId();
  table_heap = table_heap->Create(buffer_pool_manager_, table_page_id, schema, nullptr, nullptr);
  table_info->Init(table_meta, table_heap);  // 用反序列化出的table_meta和table_heap初始化table_info
  tables_[table_id] = table_info;
  table_names_[table_name] = table_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *meta_page = buffer_pool_manager_->FetchPage(page_id);  // 先获取存储索引元信息的页
  IndexMetadata *index_meta = nullptr;
  index_meta->DeserializeFrom(meta_page->GetData(), index_meta);  // 将该元信息反序列化到index_meta中
  table_id_t table_id = 0;
  table_id = index_meta->GetTableId();  // 获取表id
  TableInfo *table_info = nullptr;
  table_info = tables_[table_id];  // 获取table_info
  IndexInfo *index_info = nullptr;
  index_info->Init(index_meta, table_info, buffer_pool_manager_);  // 利用index_meta和table_info初始化index_info
  string table_name = "";
  table_name = table_info->GetTableName();  // 获取表名
  string index_name = "";
  index_name = index_meta->GetIndexName();  // 获取索引名
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = iter->second;
  return DB_SUCCESS;
}