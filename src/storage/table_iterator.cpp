#include "storage/table_iterator.h"
#include <cstddef>

#include "common/config.h"
#include "common/macros.h"
#include "page/table_page.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId &rid, Txn *txn, Row *row) {
  this->table_heap_ = table_heap;
  this->txn = txn;
  this->rid = rid;
  if (row) {
    this->row_ = new Row(*row);
  } else {
    this->row_ = nullptr;
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid = other.rid;
  txn = other.txn;
  row_ = (other.row_ ? new Row(*other.row_) : nullptr);
}

TableIterator::~TableIterator() {
  if (row_ != nullptr) {
    delete row_;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const { return itr.rid == rid; }

bool TableIterator::operator!=(const TableIterator &itr) const { return !(itr.rid == rid); }

const Row &TableIterator::operator*() { return *row_; }

Row *TableIterator::operator->() { return row_; }

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  row_ = (itr.row_ ? new Row(*itr.row_) : nullptr);
  table_heap_ = itr.table_heap_;
  rid = itr.rid;
  txn = itr.txn;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  ASSERT(row_ != nullptr, "ERROR: ++ operation on a null iterator");  // 获取当前元组所在的磁盘页
  page_id_t page_id = rid.GetPageId();
  ASSERT(page_id != INVALID_PAGE_ID, "ERROR: ++ operation on a end iterator");  // 已经到结尾，不能自增
  auto *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  ASSERT(page_id == page->GetPageId(), "ERROR: \"page_id == page->GetPageId()\" should be true");  // 简单判断一下
  RowId next_rid;  // 存储下一个rowid
  if (page->GetNextTupleRid(rid, &next_rid)) {
    row_->GetFields().clear();                             // 清空row的field，准备存储下一个row
    rid.Set(next_rid.GetPageId(), next_rid.GetSlotNum());  // 设置rid为下一个row的id
    row_->SetRowId(rid);
    table_heap_->GetTuple(row_, nullptr);  // 获取下一个row
    row_->SetRowId(rid);
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  } else {  // 可能是最后一个row，需要读取下一页
    page_id_t next_page_id = INVALID_PAGE_ID;
    while ((next_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {  // 获取下一页
      auto *next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      page = next_page;
      if (page->GetFirstTupleRid(&next_rid)) {  // 获取首个元组，失败则继续循环
        row_->GetFields().clear();
        rid = next_rid;
        row_->SetRowId(rid);
        table_heap_->GetTuple(row_, nullptr);
        row_->SetRowId(rid);
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return *this;
      }
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    rid.Set(INVALID_PAGE_ID, 0);  // rid设置无效页
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return *this;
  }
}

// iter++
TableIterator TableIterator::operator++(int) {
  Row *row_next = new Row(*(this->row_));  // 获取信息以供返回
  TableHeap *heap_next = this->table_heap_;
  RowId rid_next = this->rid;
  ++(*this);
  return TableIterator(heap_next, rid_next, nullptr, row_next);
}
