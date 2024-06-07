#include "storage/table_heap.h"
#include <cstddef>
#include <tuple>
#include "common/config.h"
#include "common/rowid.h"
#include "page/page.h"
#include "page/table_page.h"
#include "record/row.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  if (page_num > 8) {  // 如果页数比8多，从最后一页开始插入
    auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));  // 查看最后一页
    if (last_page == nullptr) {  // 如果最后一页不存在，将引用数-1并返回false
      buffer_pool_manager_->UnpinPage(last_page_id_, false);
      return false;
    }
    if (last_page->InsertTuple(row, schema_, txn, lock_manager_,
                               log_manager_)) {  // 否则尝试insert，如果成功则引用数-1，并设置为dirty
      buffer_pool_manager_->UnpinPage(last_page_id_, true);
    } else {  // 如果插入失败，则新建page
      page_id_t next_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if (new_page != nullptr && next_page_id != INVALID_PAGE_ID) {  // 新建成功
        new_page->Init(next_page_id, last_page_id_, log_manager_, txn);  // 初始化newpage,将当前lastpage作为前一个page
        last_page->SetNextPageId(next_page_id);                          // 现在next_page为last_page
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);  // 在new_page中insert
        buffer_pool_manager_->UnpinPage(last_page_id_, false);  // 将之前的lastpage引用数减一，并不设置脏页
        buffer_pool_manager_->UnpinPage(next_page_id, true);  // 现在的lastpage引用数减一，并设置为脏页
        last_page_id_ = next_page_id;                         // 更新lastpageid
        page_num++;
      } else {  // 新建失败
        buffer_pool_manager_->UnpinPage(next_page_id, false);
        buffer_pool_manager_->UnpinPage(last_page_id_, false);
        return false;
      }
    }
    return true;
  } else {  // 页数不够8，那就从首页开始往后找
    auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
    bool is_not_valid = buffer_pool_manager_->IsPageFree(GetFirstPageId());
    if (is_not_valid) {  // 如果首页为空，则新建page
      first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
    }
    if (first_page != nullptr) {
      page_num = 1;
      page_id_t page_id = GetFirstPageId();  // 获取首页id
      last_page_id_ = page_id;               // 更新lastpageid(首页是新建的所以首页就是尾页id)
      while (1) {
        if (first_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {  // 尝试将row插入当前页
          return buffer_pool_manager_->UnpinPage(page_id, true);
        } else {  // 失败则获取下一页
        auto next_page_id = first_page->GetNextPageId();
        if (next_page_id == INVALID_PAGE_ID) {  // 若下一页无效则新建下一页      
          auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
          if (new_page == nullptr || next_page_id == INVALID_PAGE_ID)  // 若新建失败，则返回false
          {
            buffer_pool_manager_->UnpinPage(page_id, false);
            return false;
          } else {  // 若创建成功
            page_num++;
            last_page_id_ = next_page_id;                              // 更新尾页
            new_page->Init(next_page_id, page_id, log_manager_, txn);  // 并初始化新页
            first_page->SetNextPageId(next_page_id);                   // 将其设置为上一页的下一页
            buffer_pool_manager_->UnpinPage(page_id, false);           // 解引用
            first_page = new_page;                                     // 更新page
            page_id = next_page_id;                                    // 更新page_id
          }
        } else {
          // 若下一页有效，获得下一页并作为当前页，继续循环
          buffer_pool_manager_->UnpinPage(page_id, false);
          page_id = next_page_id;
          first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));          }
      }
    }
    return false;
  } else {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return false;
  }
  }
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
  Row old_row;            // 定义一个row
  old_row.SetRowId(rid);  // 设置rowid
  int res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (res) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  auto page_id = rid.GetPageId();
  if (page == nullptr) {  // 如果此页不存在则不需要处理
    buffer_pool_manager_->UnpinPage(page_id, false);
    return;
  } else {  // 删除该行，并标记为脏页
    page->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  RowId rowid = row->GetRowId();
  auto page_id = rowid.GetPageId();  // 找到该row所在页
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  if (page->GetTuple(row, schema_, txn, lock_manager_)) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);
  return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t page_id = first_page_id_;  // 取出首页id
  RowId result_rid;
  while (1) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page_id == INVALID_PAGE_ID) {  // 如果页id无效，则返回end
      return End();
    }
    if (page->GetFirstTupleRid(&result_rid)) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;  // 获取成功则退出循环
    } else {
      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = page->GetNextPageId();  // 获取失败，说明该页已经无效
    }
  }
  if (page_id != INVALID_PAGE_ID) {         // 获取成功
    Row *result_row = new Row(result_rid);  // 用获取的id构造row
    GetTuple(result_row, txn);
    return TableIterator(this, result_rid, txn, result_row);
  }
  return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  RowId null;
  return TableIterator(this, null, nullptr, nullptr);
}
