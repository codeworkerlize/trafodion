
#ifndef ELEMDDLLIKEOPTIONS_H
#define ELEMDDLLIKEOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLLikeOptions.h
 * Description:  classes for options in Like clause in DDL statements
 *
 *
 * Created:      6/5/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/BaseTypes.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLLikeOpt;
class ElemDDLLikeOptWithConstraints;
class ElemDDLLikeOptWithHeadings;
class ElemDDLLikeOptWithHorizontalPartitions;
class ElemDDLLikeOptWithDivision;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLLikeOpt
// -----------------------------------------------------------------------
class ElemDDLLikeOpt : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLLikeOpt(OperatorTypeEnum operatorType = ELM_ANY_LIKE_OPT_ELEM) : ElemDDLNode(operatorType) {}

  // virtual destructor
  virtual ~ElemDDLLikeOpt();

  // cast
  virtual ElemDDLLikeOpt *castToElemDDLLikeOpt();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOpt

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutConstraints
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutConstraints : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutConstraints() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_CONSTRAINTS_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutConstraints();

  // cast
  virtual ElemDDLLikeOptWithoutConstraints *castToElemDDLLikeOptWithoutConstraints();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutConstraints

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutIndexes
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutIndexes : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutIndexes() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_INDEXES_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutIndexes();

  // cast
  virtual ElemDDLLikeOptWithoutIndexes *castToElemDDLLikeOptWithoutIndexes();
  // method for tracing
  virtual const NAString getText() const;
};

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithHeadings
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithHeadings : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithHeadings() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITH_HEADINGS_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithHeadings();

  // cast
  virtual ElemDDLLikeOptWithHeadings *castToElemDDLLikeOptWithHeadings();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithHeadings

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithHorizontalPartitions
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithHorizontalPartitions : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithHorizontalPartitions() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITH_HORIZONTAL_PARTITIONS_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithHorizontalPartitions();

  // cast
  virtual ElemDDLLikeOptWithHorizontalPartitions *castToElemDDLLikeOptWithHorizontalPartitions();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithHorizontalPartitions

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutSalt
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutSalt : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutSalt() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_SALT_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutSalt();

  // cast
  virtual ElemDDLLikeOptWithoutSalt *castToElemDDLLikeOptWithoutSalt();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutSalt

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeSaltClause
// -----------------------------------------------------------------------
class ElemDDLLikeSaltClause : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeSaltClause(ElemDDLSaltOptionsClause *saltClause)
      : ElemDDLLikeOpt(ELM_LIKE_OPT_SALT_CLAUSE_ELEM), saltClause_(saltClause) {}

  // virtual destructor
  virtual ~ElemDDLLikeSaltClause();

  // cast
  virtual ElemDDLLikeSaltClause *castToElemDDLLikeSaltClause();

  // method for tracing
  virtual const NAString getText() const;

  ElemDDLSaltOptionsClause *getSaltClause() { return saltClause_; };

 private:
  ElemDDLSaltOptionsClause *saltClause_;

};  // class ElemDDLLikeOptWithoutSalt

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutDivision
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutDivision : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutDivision() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_DIVISION_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutDivision();

  // cast
  virtual ElemDDLLikeOptWithoutDivision *castToElemDDLLikeOptWithoutDivision();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutDivision

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeLimitColumnLength
// -----------------------------------------------------------------------
class ElemDDLLikeLimitColumnLength : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeLimitColumnLength(UInt32 limit)
      : ElemDDLLikeOpt(ELM_LIKE_OPT_LIMIT_COLUMN_LENGTH), columnLengthLimit_(limit) {}

  // virtual destructor
  virtual ~ElemDDLLikeLimitColumnLength();

  // cast
  virtual ElemDDLLikeLimitColumnLength *castToElemDDLLikeLimitColumnLength();

  // method for tracing
  virtual const NAString getText() const;

  UInt32 getColumnLengthLimit() { return columnLengthLimit_; };

 private:
  UInt32 columnLengthLimit_;  // in bytes

};  // class ElemDDLLikeLimitColumnLength

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutRowFormat
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutRowFormat : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutRowFormat() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_ROW_FORMAT_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutRowFormat();

  // cast
  virtual ElemDDLLikeOptWithoutRowFormat *castToElemDDLLikeOptWithoutRowFormat();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutRowFormat

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutLobColumns
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutLobColumns : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutLobColumns() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_LOB_COLUMNS) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutLobColumns();

  // cast
  virtual ElemDDLLikeOptWithoutLobColumns *castToElemDDLLikeOptWithoutLobColumns();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutLobColumns

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutNamespace
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutNamespace : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutNamespace() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_NAMESPACE) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutNamespace();

  // cast
  virtual ElemDDLLikeOptWithoutNamespace *castToElemDDLLikeOptWithoutNamespace();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutNamespace

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithData
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithData : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithData() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITH_DATA) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithData();

  // cast
  virtual ElemDDLLikeOptWithData *castToElemDDLLikeOptWithData();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithData

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutRegionReplication
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutRegionReplication : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutRegionReplication() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_REGION_REPLICATION) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutRegionReplication();

  // cast
  virtual ElemDDLLikeOptWithoutRegionReplication *castToElemDDLLikeOptWithoutRegionReplication();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutConstraints

// -----------------------------------------------------------------------
// definition of class ElemDDLLikeOptWithoutRegionReplication
// -----------------------------------------------------------------------
class ElemDDLLikeOptWithoutIncrBackup : public ElemDDLLikeOpt {
 public:
  // constructor
  ElemDDLLikeOptWithoutIncrBackup() : ElemDDLLikeOpt(ELM_LIKE_OPT_WITHOUT_INCREMENTAL_BACKUP) {}

  // virtual destructor
  virtual ~ElemDDLLikeOptWithoutIncrBackup();

  // cast
  virtual ElemDDLLikeOptWithoutIncrBackup *castToElemDDLLikeOptWithoutIncrBackup();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLikeOptWithoutConstraints

#endif  // ELEMDDLLIKEOPTIONS_H
