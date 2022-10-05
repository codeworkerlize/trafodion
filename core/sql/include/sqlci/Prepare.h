
#ifndef PREPARE_H
#define PREPARE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Prepare.h
 * RCS:          $Id: Prepare.h,v 1.7 1998/09/07 21:49:50  Exp $
 * Description:
 *
 *
 * Created:      4/15/95
 * Modified:     $ $Date: 1998/09/07 21:49:50 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *
 *****************************************************************************
 */

#include "cli/SQLCLIdev.h"
#include "sqlci/SqlciDefs.h"

// The following class was named as Entry before. I renamed it to PrepEntry
// because the file btree.h defines another class named Entry and the Tandem
// linker sometimes resolves the destructor to the wrong one. -- Freda Xu
class PrepEntry {
  int datatype_;
  int length_;
  int scale_;
  int precision_;
  int nullFlag_;
  int vcIndLen_;
  char *heading_;
  int headingLen_;
  char *outputName_;
  int outputNameLen_;
  int displayLen_;
  int displayBufLen_;
  int charsetEnum_;
  char *charset_;
  char *tableName_;
  int tableLen_;
  char *varPtr_;
  char *indPtr_;

 public:
  PrepEntry(int datatype, int length, int scale, int precision, int nullFlag, int vcIndLen, char *heading_,
            int headingLen, char *outputName, int outputNameLen_, int displayLen_, int displayBufLen_,
            int charsetEnum_, char *tableName, int tableLen);
  ~PrepEntry();

  int datatype() { return datatype_; }
  int length() { return length_; }
  int scale() { return scale_; }
  int precision() { return precision_; }
  int nullFlag() { return nullFlag_; }
  int vcIndLen() { return vcIndLen_; }
  char *heading() { return heading_; }
  int headingLen() { return headingLen_; }
  char *outputName() { return outputName_; }
  int outputNameLen() { return outputNameLen_; }
  int displayLen() { return displayLen_; }
  int displayBufLen() { return displayBufLen_; }
  int charsetEnum() { return charsetEnum_; }
  char *charset() { return charset_; }
  char *tableName() { return tableName_; }
  int tableLen() { return tableLen_; }
  char *varPtr() { return varPtr_; }
  char *indPtr() { return indPtr_; }
  void setVarPtr(char *vp) { varPtr_ = vp; }
  void setIndPtr(char *ip) { indPtr_ = ip; }
};

////////////////////////////////////////////////////
class PrepStmt {
  friend class PreparedStmts;

  char *stmt_name;
  int stmt_name_len;

  char *stmt_str;
  int stmt_str_len;

  SQLDESC_ID *sql_src;

  SQLSTMT_ID *stmt;
  SQLDESC_ID *input_desc;
  SQLDESC_ID *output_desc;
  dml_type type;

  int queryType_;
  int subqueryType_;

  int numInputEntries_;
  int numOutputEntries_;

  PrepEntry **outputEntries_;
  PrepEntry **inputEntries_;

  // space to fetch the selected row.
  int outputDatalen_;
  char *outputData_;

  // this is where the formatted output row will be created.
  int outputBuflen_;
  char *outputBuf_;

  // query id for this query
  char *uniqueQueryId_;
  int uniqueQueryIdLen_;

 public:
  PrepStmt(const char *stmt_name_, dml_type type_ = DML_SELECT_TYPE);
  ~PrepStmt();
  void set(const char *str_, SQLDESC_ID *sql_src_, SQLSTMT_ID *stmt_, int numInputEntries, SQLDESC_ID *input_desc_,
           int numOutputEntries, SQLDESC_ID *output_desc_);

  void remove();

  inline char *getStmtName() { return stmt_name; };
  int getStmtNameLen() { return stmt_name_len; };

  inline SQLDESC_ID *getSqlSrc() { return sql_src; };
  inline SQLDESC_ID *getInputDesc() { return input_desc; };
  inline SQLDESC_ID *getOutputDesc() { return output_desc; };
  inline SQLSTMT_ID *getStmt() { return stmt; };

  inline char *getStr() { return stmt_str; };
  inline int getStrLen() { return stmt_str_len; };

  inline dml_type getType() { return type; };

  short contains(const char *value) const;

  int &queryType() { return queryType_; }
  void setSubqueryType(int subqueryType) { subqueryType_ = subqueryType; }
  int getSubqueryType() { return subqueryType_; }

  int numInputEntries() { return numInputEntries_; }
  int numOutputEntries() { return numOutputEntries_; }

  void setOutputDesc(int n, SQLDESC_ID *desc) {
    numOutputEntries_ = n;
    output_desc = desc;
  }

  PrepEntry **&outputEntries() { return outputEntries_; }
  PrepEntry **&inputEntries() { return inputEntries_; }

  int &outputDatalen() { return outputDatalen_; }
  char *&outputData() { return outputData_; }

  int &outputBuflen() { return outputBuflen_; }
  char *&outputBuf() { return outputBuf_; }

  int &uniqueQueryIdLen() { return uniqueQueryIdLen_; }
  char *&uniqueQueryId() { return uniqueQueryId_; }
};

class CursorStmt {
 public:
  CursorStmt(char *cursorName, SQLSTMT_ID *cursorStmtId, PrepStmt *stmt, Int16 internallyPrepared = -1);
  ~CursorStmt();

  char *cursorName() { return cursorName_; };
  SQLSTMT_ID *cursorStmtId() { return cursorStmtId_; };
  PrepStmt *prepStmt() { return stmt_; };
  short contains(const char *value) const;
  Int16 internallyPrepared() { return internallyPrepared_; };
  int getResultSetIndex() const { return resultSetIndex_; }
  void setResultSetIndex(int i) { resultSetIndex_ = i; }

 private:
  char *cursorName_;
  int cursorNameLen_;
  SQLSTMT_ID *cursorStmtId_;
  PrepStmt *stmt_;
  Int16 internallyPrepared_;
  int resultSetIndex_;
};

class PreparedStmts {
  PrepStmt *prep_stmt_hdr;

 public:
  PreparedStmts();
  ~PreparedStmts();

  void addPrepStmt(PrepStmt *in_prep_stmt);
  PrepStmt *getPrepStmt(const char *stmt_name_);
  void removePrepStmt(const char *stmt_name_);
};

#endif
