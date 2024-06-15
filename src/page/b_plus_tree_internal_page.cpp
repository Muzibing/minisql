#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetPageId(page_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */

/*
 * pairs_off + index * pair_size + key_off : pairs[index].key
 * pairs_off + index * pair_size + val_off : pairs[index].val
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == value) return i;
  }
  return -1;  // There is no pair matches value
}

// beginning of the pair of index
void *InternalPage::PairPtrAt(int index) { return KeyAt(index); }

// Copy pair_num pairs from src to dest
void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  // pari_size = GetKeySize() + sizeof(page_id_t)
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)

 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  //  LOG(INFO) << "InternalPage::Lookup, key = " << key << std::endl;
  // index from 0 to GetSize()-1
  int index = 0;
  int left = 1, right = GetSize() - 1;
  // binary search
  while (left <= right) {
    int mid = (left + right) / 2;
    int ret = KM.CompareKeys(key, KeyAt(mid));
    if (ret == 0) {
      index = mid;
      break;
    } else if (ret < 0) {  // key < mid->key
      right = mid - 1;
    } else {
      index = mid;
      left = mid + 1;
    }
  }
  return ValueAt(index);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);  // 包含一个旧值和一个新值
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
/*
 *  Update the size
 *  QUE: Are there any other things to be updated?
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int pos = ValueIndex(old_value) + 1;  // 找到插入位置
  //  LOG(INFO) << "InsertNodeAfter " << old_value << " " << new_value << std::endl;
  PairCopy(PairPtrAt(pos + 1), PairPtrAt(pos), GetSize() - pos);  // 将插入位置之后的所有键值对向后移动
  SetKeyAt(pos, new_key);
  SetValueAt(pos, new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 *
 * Move the first half keys or the second half keys?
 * I may try to move the second half.
 * Suppose that I will move the to an empty page.
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  ASSERT(recipient != nullptr, "No recipient available");
  int half_size = GetSize() / 2;
  recipient->CopyNFrom(PairPtrAt(GetSize() - half_size), half_size, buffer_pool_manager);
  IncreaseSize(-half_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  //    LOG(INFO) << "InternalPage::CopyNFrom size = " << size << std::endl;
  PairCopy(PairPtrAt(GetSize()), src, size);
  IncreaseSize(size);
  for (int i = 1; i <= size; i++) {
    int page_id = ValueAt(GetSize() - i);
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(page_id)->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  if (index < 0 || index >= GetSize()) {
  }
  PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - index - 1);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  ASSERT(GetSize() == 1, "InternalPage::RemoveAndReturnOnlyChild size must equals 1");
  page_id_t child_value = ValueAt(0);
  SetSize(0);
  return child_value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);
  //  auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(GetParentPageId())->GetData());
  //  int pos = parent->ValueIndex(GetPageId());
  //  parent->SetKeyAt(pos, KeyAt(0));
  //  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetValueAt(GetSize(), value);
  SetKeyAt(GetSize(), key);
  IncreaseSize(1);
  // 从缓冲区获取子节点
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(GetPageId());     // 设置子页面的父页面ID为当前页面ID
  buffer_pool_manager->UnpinPage(value, true);  // 子页面已修改并解除pin
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
  recipient->SetKeyAt(0, KeyAt(GetSize() - 1));
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize());  // 复制第一个元素到当前页面第一个位置
  IncreaseSize(1);
  SetValueAt(0, value);
  // 加载被复制元素所在页面
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(GetPageId());     // 更新被复制元素页面的父节点id
  buffer_pool_manager->UnpinPage(value, true);  // 被复制元素页面已经修改并解除pin
}
