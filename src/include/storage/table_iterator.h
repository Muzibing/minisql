#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/config.h"
#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

class TableHeap;

class TableIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), rid(rid), txn(txn) {}
  explicit TableIterator(TableHeap *table_heap, RowId &rid, Txn *txn, Row *row);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

 private:
  // add your own private member variables here
  Row *row_{nullptr};
  TableHeap *table_heap_{nullptr};
  RowId rid{INVALID_PAGE_ID, 0};
  Txn *txn;
};

#endif  // MINISQL_TABLE_ITERATOR_H
