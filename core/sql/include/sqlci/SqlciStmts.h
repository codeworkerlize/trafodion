#ifndef SQLCISTMTS_H
#define SQLCISTMTS_H

class StmtEntry;
class InputStmt;

class SqlciStmts {
  StmtEntry *First;
  StmtEntry *Last;
  int last_stmt_num;
  int max_entries;

 public:
  SqlciStmts(int max_entries_);
  ~SqlciStmts();
  void add(InputStmt *input_stmt_);
  void display(int num_stmts_) const;
  InputStmt *get(int stmt_num_) const;
  InputStmt *get(char *stmt_) const;
  InputStmt *get() const;  // gets last statement
  void remove();           // removes last statement
  int last_stmt() const { return last_stmt_num; }
};

class StmtEntry {
  int stmt_num;
  InputStmt *input_stmt;
  StmtEntry *next;
  StmtEntry *prev;

 public:
  StmtEntry();
  ~StmtEntry();
  void disconnect();
  void set(int stmt_num_, InputStmt *input_stmt_);
  inline int getStmtNum() const { return stmt_num; }
  inline InputStmt *getInputStmt() const { return input_stmt; }
  inline StmtEntry *getNext() const { return next; }
  inline StmtEntry *getPrev() const { return prev; }
  inline void setNext(StmtEntry *next_) { next = next_; }
  inline void setPrev(StmtEntry *prev_) { prev = prev_; }
};

#endif
