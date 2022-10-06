

#include <stdlib.h>
#include <ctype.h>

#include "common/ComASSERT.h"
#include "export/ComDiags.h"
#include "sqlci/SqlciStmts.h"
#include "sqlci/SqlciNode.h"
#include "sqlci/SqlciCmd.h"
#include "sqlci/SqlciError.h"
#include "sqlci/SqlciParser.h"
#include "sqlci/InputStmt.h"
#include "common/str.h"

extern ComDiagsArea sqlci_DA;

SqlciStmts::SqlciStmts(int max_entries_) {
  last_stmt_num = 0;

  max_entries = max_entries_;

  First = new StmtEntry();

  StmtEntry *curr = NULL;
  StmtEntry *prev = First;

  for (short i = 1; i < max_entries; i++) {
    curr = new StmtEntry();

    prev->setNext(curr);
    curr->setPrev(prev);

    prev = curr;
  }

  if (curr != NULL) {
    curr->setNext(First);
    First->setPrev(curr);
    Last = First;
  }
}

SqlciStmts::~SqlciStmts() {
  StmtEntry *curr = First;
  StmtEntry *next = NULL;

  for (short i = 1; i < max_entries; i++) {
    next = curr->getNext();
    delete curr;
    curr = next;
  }

  if (next != NULL) delete next;
}

void SqlciStmts::add(InputStmt *input_stmt_) {
  if (!input_stmt_->isEmpty()) {
    // input_stmt is not a comment string.
    // Go ahead and add it to the the history list.
    // Otherwise, just ignore it.

    Last->set(++last_stmt_num, input_stmt_);  // deletes prev InputStmt
    input_stmt_->setInHistoryList(TRUE);      // mark the stmt just added

    Last = Last->getNext();

    if (Last == First) First = First->getNext();
  }
}

InputStmt *SqlciStmts::get(int stmt_num_) const {
  if (First == Last)  // empty, no statements added yet
    return 0;

  if ((stmt_num_ < First->getStmtNum()) || (stmt_num_ > Last->getPrev()->getStmtNum())) return 0;

  StmtEntry *curr = First;

  while ((curr != Last) && (curr->getStmtNum() != stmt_num_)) curr = curr->getNext();

  if (curr != Last)
    return curr->getInputStmt();
  else
    return 0;
}

InputStmt *SqlciStmts::get(char *input_string_) const {
  if (First == Last)  // empty, no statements added yet
    return 0;

  size_t input_len = strlen(input_string_);

  StmtEntry *curr = Last;

  while (curr != First) {
    char *curr_string = curr->getPrev()->getInputStmt()->getPackedString();

    if (input_len <= strlen(curr_string))
      if (strncmp(input_string_, curr_string, input_len) == 0)
        return curr->getPrev()->getInputStmt();
      else {
        // input_string_ is a single SQLCI stmt keyword, so we don't have
        // to worry about quoted literals and delimited identifiers now
        // while we do a quick case-insensitive comparison
        size_t i = 0;
        for (i = 0; i < input_len; i++)
          if (toupper(input_string_[i]) != toupper(curr_string[i])) break;
        if (i == input_len) return curr->getPrev()->getInputStmt();
      }

    curr = curr->getPrev();
  }

  return 0;  // not found
}

// get last statement
InputStmt *SqlciStmts::get() const {
  if (First == Last)  // empty, no statements added yet
    return 0;

  return Last->getPrev()->getInputStmt();
}

// remove most recent stmt (FC or !) from history list,
// but do not delete it.
// (see StmtEntry::disconnect below)
void SqlciStmts::remove() {
  if (First == Last)  // empty, no statements added yet
    return;

  Last = Last->getPrev();
  last_stmt_num--;

  InputStmt *input_stmt_ = Last->getInputStmt();
  ComASSERT(input_stmt_);
  input_stmt_->setInHistoryList(FALSE);  // out of list, for future delete
  Last->disconnect();
}

void SqlciStmts::display(int num_stmts_) const {
  if (num_stmts_ >= max_entries) num_stmts_ = max_entries - 1;

  StmtEntry *curr = Last;

  int i = 0;

  while ((curr != First) && (i < num_stmts_)) {
    curr = curr->getPrev();
    i++;
  }

  while (curr != Last) {
    curr->getInputStmt()->display(curr->getStmtNum());
    curr = curr->getNext();
  }
}

///////////////////////////////////////////////////////
// class StmtEntry
///////////////////////////////////////////////////////
StmtEntry::StmtEntry() {
  stmt_num = 0;
  input_stmt = 0;
  next = 0;
  prev = 0;
}

StmtEntry::~StmtEntry() {
  delete input_stmt;  // delete regardless of stmt's "inHistoryList" setting
}

void StmtEntry::disconnect() {
  // disconnect the just-inserted FC/! command from history list
  // for later deleting in main SqlciEnv interpreter loop
  // (must wait to delete it till returned there because main loop
  // retains a pointer to it and we don't want to leave any dangling pointers
  // which would happen if we deleted the stmt now)
  ComASSERT(input_stmt && !input_stmt->isInHistoryList());
  input_stmt = 0;
}

void StmtEntry::set(int stmt_num_, InputStmt *input_stmt_) {
  stmt_num = stmt_num_;
  // overwrite the n'th previous stmt in the circular history list
  delete input_stmt;  // delete regardless of stmt's "inHistoryList" setting
  input_stmt = input_stmt_;
}

