#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.   **/
  struct dirent *stdir;
  while ((stdir = readdir(dir)) != nullptr) {
    if (strcmp(stdir->d_name, ".") == 0 || strcmp(stdir->d_name, "..") == 0 || stdir->d_name[0] == '.') continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}
// 构建火山模型
std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database" << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {  // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  std::string table_name = ast->child_->val_;     // 获取表名
  pSyntaxNode pAst = ast->child_->next_->child_;  // 获取表的属性相关定义语法树节点

  TableInfo *table_info(nullptr);      // 创建一个新的TableInfo
  std::vector<Column *> columns;       // 表的列
  std::vector<string> unique_columns;  // Unique的列
  std::vector<string> primary_key;     // 主键索引列
  CatalogManager *catalog_manager = context->GetCatalog();
  uint32_t index = 0;  // 列的下标

  while (pAst != nullptr) {
    switch (pAst->type_)  // 根据节点的不同类型执行不同操作
    {
      case kNodeColumnDefinition:  // 属性的相关定义，儿子节点包括属性名，类型，当前节点value决定是否Unique
      {
        std::string column_name = pAst->child_->val_;  // 获取属性名

        std::string column_type_ = pAst->child_->next_->val_;  // 获取属性类型
        TypeId type;
        int32_t length = 0;
        bool uniqueFlag = (pAst->val_ != nullptr);  // 因为只设置了unique，所以非空时为unique
        if (uniqueFlag) unique_columns.emplace_back(column_name);  // 把Unique标注的列加入Unique需要创建的索引数组
        if (column_type_ == "int") {
          type = kTypeInt;
          Column *column = new Column(column_name, type, index++, true, uniqueFlag);
          columns.emplace_back(column);
        } else if (column_type_ == "float") {
          type = kTypeFloat;
          Column *column = new Column(column_name, type, index++, true, uniqueFlag);
          columns.emplace_back(column);
        } else if (column_type_ == "char") {
          type = kTypeChar;
          // 对于小于0的数处理为0，大于0的小数进行截断处理
          length = max(int32_t(std::stod(pAst->child_->next_->child_->val_)), 0);
          Column *column = new Column(column_name, type, length, index++, true, uniqueFlag);
          columns.emplace_back(column);
        }
        break;
      }
      case kNodeColumnList:  // 定义主键
      {
        auto key = pAst->child_;
        while (key != nullptr) {
          primary_key.emplace_back(key->val_);  // 将主键的属性加入主键数组
          key = key->next_;
        }
        break;
      }
      default:
        break;
    }
    pAst = pAst->next_;
  }
  // 创建表

  Schema *schema = new Schema(columns);
  Txn *transaction = context->GetTransaction();

  if (catalog_manager->CreateTable(table_name, schema, transaction, table_info) == DB_TABLE_ALREADY_EXIST)
    return DB_ALREADY_EXIST;  // 表已经存在

  // 默认使用btree进行索引
  // 对每一个Unique列创建索引
  for (const auto &iter : unique_columns) {
    string index_name = iter + "_UNIQUE";
    IndexInfo *index_info(nullptr);
    catalog_manager->CreateIndex(table_name, index_name, vector<string>{iter}, transaction, index_info, "btree");
  }
  // 对主键创建索引
  if (!primary_key.empty()) {
    string index_name = "PRIMARY_KEY";
    IndexInfo *index_info(nullptr);
    catalog_manager->CreateIndex(table_name, index_name, primary_key, transaction, index_info, "btree");
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {  // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  std::string table_name = ast->child_->val_;
  return catalog_manager->DropTable(table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {  // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;  // 存储当前数据库的所有表信息
  CatalogManager *catalog_manager = context->GetCatalog();
  if (catalog_manager->GetTables(tables) == DB_FAILED) {  // 当前数据库没有表
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  bool flag = false;
  string table_title("Table name");  // 第一列的名字
  string index_title("Index name");  // 第二列的名字
  uint max_width_table = table_title.length();
  uint max_width_index = index_title.length();
  // 获取表名长度最大值和索引名长度最大值
  for (const auto &iter : tables) {
    vector<IndexInfo *> indexes;
    string table_name = iter->GetTableName();
    catalog_manager->GetTableIndexes(table_name, indexes);  // 获取该表的所有索引信息
    if (iter->GetTableName().length() > max_width_table) max_width_table = iter->GetTableName().length();
    for (const auto &iter_ : indexes) {
      if (iter_->GetIndexName().length() > max_width_index) max_width_index = iter_->GetIndexName().length();
      flag = true;
    }
  }
  if (!flag) {  // 没有索引
    cout << "No index (0.00 sec)" << endl;
    return DB_FAILED;
  }
  // 格式化输出
  cout << "+" << setfill('-') << setw(max_width_table + 2) << "" << "+";
  cout << setfill('-') << setw(max_width_index + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width_table) << table_title << " |";
  cout << "| " << std::left << setfill(' ') << setw(max_width_index) << index_title << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width_table + 2) << "" << "+";
  cout << setfill('-') << setw(max_width_index + 2) << "" << "+" << endl;

  for (const auto iter : tables) {
    vector<IndexInfo *> indexes;
    string table_name = iter->GetTableName();
    catalog_manager->GetTableIndexes(table_name, indexes);
    for (const auto &iter_ : indexes) {
      cout << "| " << std::left << setfill(' ') << setw(max_width_table) << iter->GetTableName() << " |";
      cout << "| " << std::left << setfill(' ') << setw(max_width_index) << iter_->GetIndexName() << " |" << endl;
    }
  }
  cout << "+" << setfill('-') << setw(max_width_table + 2) << "" << "+";
  cout << setfill('-') << setw(max_width_index + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {  // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  std::string index_name = ast->child_->val_;               // 获取索引名
  std::string table_name = ast->child_->next_->val_;        // 获取表名
  pSyntaxNode pColumn = ast->child_->next_->next_->child_;  // 获取索引基于的列
  string index_type("btree");                               // 若用户没有具体指明，默认是btree
  if (ast->child_->next_->next_->next_ != nullptr)          // 用户指定了索引类型
    index_type = ast->child_->next_->next_->next_->child_->val_;
  IndexInfo *index_info(nullptr);
  vector<string> index_keys;
  Txn *txn = context->GetTransaction();
  while (pColumn != nullptr) {  // 遍历搜索码的组成
    index_keys.emplace_back(pColumn->val_);
    pColumn = pColumn->next_;
  }
  return catalog_manager->CreateIndex(table_name, index_name, index_keys, txn, index_info, index_type);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  // drop indexes; 仿佛是从所有表中删除同名索引
  if (current_db_.empty()) {  // 还没有选择数据库
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  string index_name = ast->child_->val_;
  bool deleteFlag = false;  // 标记是否至少存在一个同名索引
  vector<TableInfo *> table_infos;
  catalog_manager->GetTables(table_infos);  // 获取数据库中所有表的信息
  for (const auto &itr : table_infos) {     // 遍历所有表
    vector<IndexInfo *> index_infos;
    catalog_manager->GetTableIndexes(itr->GetTableName(), index_infos);  // 获得该表的所有索引
    for (const auto &itr_ : index_infos) {
      if (itr_->GetIndexName() == index_name) {  // 名字相同则删除索引
        catalog_manager->DropIndex(itr->GetTableName(), index_name);
        deleteFlag = true;
      }
    }
  }
  if (!deleteFlag) {
    return DB_INDEX_NOT_FOUND;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // 当前文件的目录——cmake_build_debug_wsl>bin
  // 重定向输入流到文件中执行sql语句，在main中：若文件到头了，重定向回终端输入
  // file_start_time = std::chrono::system_clock::now();  // 记录文件开始读的时间
  freopen(ast->child_->val_, "r", stdin);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
