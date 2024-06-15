#include "index/b_plus_tree.h"

#include <ostream>
#include <string>

#include "common/config.h"
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // 获取索引的根页面
  auto index_root_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (index_root_page->GetRootId(index_id, &root_page_id_)) {  // 成功获取到根页面id
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    root_page_id_ = INVALID_PAGE_ID;
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    // buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (IsEmpty()) return;
  if (current_page_id == INVALID_PAGE_ID) {  // 树已经被销毁
    current_page_id = root_page_id_;
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(2);
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  if (!page->IsLeafPage()) {  // 递归销毁所有子页面
    auto *inner = reinterpret_cast<InternalPage *>(page);
    for (int i = 0; i < inner->GetSize(); i++) {
      Destroy(inner->ValueAt(i));
    }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // 先在缓冲池中取消固定
  buffer_pool_manager_->DeletePage(page->GetPageId());        // 删除该页面
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) return false;
  // std::cerr << "0:okkkkk" << std::endl;
  auto *page = FindLeafPage(key, INVALID_PAGE_ID, false);  // 尝试寻找包含指定键的叶子页面
  if (page == nullptr) {
    LOG(INFO) << "GetValue() : FindLeafPage Error";
    return false;
  }
  // std::cerr << "1:okkkkk" << std::endl;
  // 将找到的页面转为叶页面类型
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  RowId ret;
  bool find = leaf->Lookup(key, ret, processor_);
  // std::cerr << "2:okkkkk" << std::endl;
  if (find) {  // 找到键对应的值则添加到结果向量
    result.push_back(ret);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    // std::cerr << "3:okkkkk" << std::endl;
    return true;
  }
  // 取消固定子页面
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  // std::cerr << "4:okkkkk" << std::endl;
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto *page = buffer_pool_manager_->NewPage(root_page_id_);  // 分配一个新页作为根页面
  if (page == nullptr) {
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_max_size_ = (4064) / (processor_.GetKeySize() + sizeof(value)) - 1;
  internal_max_size_ = leaf_max_size_;
  if (internal_max_size_ < 2) {  // 最大大小不能小于2
    internal_max_size_ = 2;
    leaf_max_size_ = 2;
  }
  leaf->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf->Insert(key, value, processor_);  // 在新的叶页面插入键值对
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(1);  // 更新页面ID，已经初始化并插入第一个元素
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  RowId _value;  // 存储可能查到的结果
  auto *page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false)->GetData());
  if (page->Lookup(key, _value, processor_)) {  // 如果键已经存在则返回
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  page->Insert(key, value, processor_);         // 在叶节点插入键值对
  if (page->GetSize() >= page->GetMaxSize()) {  // 此时需要拆分页面
    auto *new_page = Split(page, transaction);
    new_page->SetNextPageId(page->GetNextPageId());
    page->SetNextPageId(new_page->GetPageId());                         // 原页面下一个id为新页面
    InsertIntoParent(page, new_page->KeyAt(0), new_page, transaction);  // 在父节点插入拆分键
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);  // 取消固定
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;  // 新建页面用于存储被分裂的元素
  auto *page = buffer_pool_manager_->NewPage(new_page_id);
  if (page == nullptr) {
    return nullptr;  // 无法获取新页面
  }
  BPlusTreeInternalPage *new_page = reinterpret_cast<InternalPage *>(page);
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_page, buffer_pool_manager_);  // 移动一半
  return new_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;  // 类似内部节点的分裂
  auto *page = buffer_pool_manager_->NewPage(new_page_id);
  if (page == nullptr) {
    return nullptr;
  }
  BPlusTreeLeafPage *new_node = reinterpret_cast<LeafPage *>(page);
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_node);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {  // 旧节点是根节点则新创一个父节点作为根节点
    auto *page = buffer_pool_manager_->NewPage(root_page_id_);  // 创建一个新页面来作为新的根节点
    if (page == nullptr) {
    }
    auto *root_page = reinterpret_cast<InternalPage *>(page->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());  // 在根节点插入旧节点和新节点的引用
    old_node->SetParentPageId(root_page_id_);                                       // 设置父节点
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(0);
  } else {  // 否则获取旧节点的父节点，在父节点中插入新节点
    auto *parent_page = reinterpret_cast<BPlusTree::InternalPage *>(
        buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (parent_page->GetSize() >= parent_page->GetMaxSize()) {  // 如果父节点过大需要分裂
      InternalPage *parent_split_page = Split(parent_page, transaction);
      InsertIntoParent(parent_page, parent_split_page->KeyAt(0), parent_split_page, transaction);
      // 解锁被分裂的节点
      buffer_pool_manager_->UnpinPage(parent_split_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // 解锁父节点
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto *leaf =
      reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false)->GetData());  // 查找包含当前目标的叶子页
  int pre_size = leaf->GetSize();
  if (pre_size > leaf->RemoveAndDeleteRecord(key, processor_)) {
    CoalesceOrRedistribute(leaf, transaction);  // 如果还有元素则需要合并或重新分布
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);  // 叶子页大小发生变化
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  bool _delete = false;
  if (node->IsRootPage()) {
    _delete = AdjustRoot(node);  // 根节点特殊处理
  } else if (node->GetSize() >= node->GetMinSize()) {
    return _delete;
  } else {
    page_id_t parent_id = node->GetParentPageId();
    auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    int index = parent_page->ValueIndex(node->GetPageId());
    int sibling_index = index - 1;  // 当前节点的兄弟节点的索引
    if (index == 0) {
      sibling_index = 1;  // 如果当前节点是0，则兄弟节点是1
    }
    page_id_t sibling_id = parent_page->ValueAt(sibling_index);
    auto *sibling =
        reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(sibling_id)->GetData());  // 从缓存池获取兄弟节点
    if (sibling->GetSize() + node->GetSize() < node->GetMaxSize()) {                    // 小于最大尺寸则合并
      Coalesce(sibling, node, parent_page, index);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      _delete = true;
    } else {  // 节点重新分布
      Redistribute(sibling, node, index);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    }
  }
  return _delete;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int sib_index = index == 0 ? 1 : index - 1;  // 确定兄弟节点的索引
  if (index < sib_index) {                     // 合并操作从当前节点开始
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
    // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(sib_index);
  } else {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);  // 递归处理尝试进一步合并或重新分布
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {  // 类似上一个函数
  int sib_index = index == 0 ? 1 : index - 1;
  if (index < sib_index) {
    neighbor_node->MoveAllTo(node, parent->KeyAt(sib_index), buffer_pool_manager_);
    // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(sib_index);
  } else {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {  // 当前是第一个节点，相邻节点的第一个元素移动到当前节点末尾
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {  // 相邻节点最后一个元素移到当前节点开头
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {  // 类似上一个函数
  auto *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {  // case 1
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  } else if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {  // case 2
    auto root = reinterpret_cast<InternalPage *>(old_root_node);
    auto *new_root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root->ValueAt(0))->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root->GetPageId();  // 该节点作为新的根节点
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto *page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, INVALID_PAGE_ID, true)->GetData());
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);         // 不再需要pin该页面
  return IndexIterator(page_id, buffer_pool_manager_, 0);  // 返回一个迭代器用于初始遍历B+树
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto *page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID, false)->GetData());
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, page->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { return IndexIterator(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (root_page_id_ == INVALID_PAGE_ID) return nullptr;
  if (page_id == INVALID_PAGE_ID) {  // 如果没有指定初始页面id则从根页面开始
    page_id = root_page_id_;
  }
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  while (!page->IsLeafPage()) {  // 不是叶子页面则继续向下查找
    auto inner = reinterpret_cast<InternalPage *>(page);
    page_id_t child_id;
    if (leftMost) {
      child_id = inner->ValueAt(0);
    } else {
      child_id = inner->Lookup(key, processor_);
    }
    auto child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_id)->GetData());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child;  // 更新当前页面为子页面
  }
  return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto *root = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (insert_record == 1) {  // 已经初始化并插入第一个元素
    ASSERT(root->Insert(index_id_, root_page_id_), "BPlusTree::UpdateRootPageId() inserted failed");
  } else if (insert_record == 0) {
    ASSERT(root->Update(index_id_, root_page_id_), "BPlusTree::UpdateRootPageId() updated failed");
  } else {
    ASSERT(root->Delete(index_id_), "BPlusTree::UpdateRootPageId() deleted failed");
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">" << "max_size=" << leaf->GetMaxSize()
        << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">" << "max_size=" << inner->GetMaxSize()
        << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}