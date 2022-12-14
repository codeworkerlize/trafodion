/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ElemDDLLikeOptions.C
 * Description:  methods for class ElemDDLLikeOpt and any classes
 *               derived from class ElemDDLLikeOpt.
 *
 *               Please note that class ElemDDLLikeOpt is not derived
 *               from class ElemDDLLike.  Class ElemDDLLike and classes
 *               derived from class ElemDDLLike are defined in files
 *               ElemDDLLike.C, ElemDDLLike.h, ElemDDLLikeCreateTable,
 *               etc.
 *
 * Created:      6/5/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLLikeOptions.h"

#include "parser/ElemDDLSaltOptions.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOpt
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOpt::~ElemDDLLikeOpt() {}

// casting
ElemDDLLikeOpt *ElemDDLLikeOpt::castToElemDDLLikeOpt() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOpt::getText() const {
  ABORT("internal logic error");
  return "ElemDDLLikeOpt";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutConstraints
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutConstraints::~ElemDDLLikeOptWithoutConstraints() {}

// casting
ElemDDLLikeOptWithoutConstraints *ElemDDLLikeOptWithoutConstraints::castToElemDDLLikeOptWithoutConstraints() {
  return this;
}

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutConstraints::getText() const { return "ElemDDLLikeOptWithoutConstraints"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutIndexes
// -----------------------------------------------------------------------
ElemDDLLikeOptWithoutIndexes::~ElemDDLLikeOptWithoutIndexes() {}

// casting
ElemDDLLikeOptWithoutIndexes *ElemDDLLikeOptWithoutIndexes::castToElemDDLLikeOptWithoutIndexes() { return this; }

//
// methods for tracing
//
const NAString ElemDDLLikeOptWithoutIndexes::getText() const { return "ElemDDLLikeOptWithoutIndexes"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithHeadings
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithHeadings::~ElemDDLLikeOptWithHeadings() {}

// casting
ElemDDLLikeOptWithHeadings *ElemDDLLikeOptWithHeadings::castToElemDDLLikeOptWithHeadings() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithHeadings::getText() const { return "ElemDDLLikeOptWithHeadings"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithHorizontalPartitions
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithHorizontalPartitions::~ElemDDLLikeOptWithHorizontalPartitions() {}

// casting
ElemDDLLikeOptWithHorizontalPartitions *
ElemDDLLikeOptWithHorizontalPartitions::castToElemDDLLikeOptWithHorizontalPartitions() {
  return this;
}

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithHorizontalPartitions::getText() const {
  return "ElemDDLLikeOptWithHorizontalPartitions";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutSalt
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutSalt::~ElemDDLLikeOptWithoutSalt() {}

// casting
ElemDDLLikeOptWithoutSalt *ElemDDLLikeOptWithoutSalt::castToElemDDLLikeOptWithoutSalt() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutSalt::getText() const { return "ElemDDLLikeOptWithoutSalt"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeSaltClause
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeSaltClause::~ElemDDLLikeSaltClause() { delete saltClause_; }

// casting
ElemDDLLikeSaltClause *ElemDDLLikeSaltClause::castToElemDDLLikeSaltClause() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeSaltClause::getText() const { return "ElemDDLLikeSaltClause"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutDivision
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutDivision::~ElemDDLLikeOptWithoutDivision() {}

// casting
ElemDDLLikeOptWithoutDivision *ElemDDLLikeOptWithoutDivision::castToElemDDLLikeOptWithoutDivision() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutDivision::getText() const { return "ElemDDLLikeOptWithoutDivision"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeLimitColumnLength
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeLimitColumnLength::~ElemDDLLikeLimitColumnLength() {}

// casting
ElemDDLLikeLimitColumnLength *ElemDDLLikeLimitColumnLength::castToElemDDLLikeLimitColumnLength() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeLimitColumnLength::getText() const { return "ElemDDLLikeLimitColumnLength"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutRowFormat
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutRowFormat::~ElemDDLLikeOptWithoutRowFormat() {}

// casting
ElemDDLLikeOptWithoutRowFormat *ElemDDLLikeOptWithoutRowFormat::castToElemDDLLikeOptWithoutRowFormat() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutRowFormat::getText() const { return "ElemDDLLikeOptWithoutRowFormat"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutLobColumns
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutLobColumns::~ElemDDLLikeOptWithoutLobColumns() {}

// casting
ElemDDLLikeOptWithoutLobColumns *ElemDDLLikeOptWithoutLobColumns::castToElemDDLLikeOptWithoutLobColumns() {
  return this;
}

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutLobColumns::getText() const { return "ElemDDLLikeOptWithoutLobColumns"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithData
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithData::~ElemDDLLikeOptWithData() {}

// casting
ElemDDLLikeOptWithData *ElemDDLLikeOptWithData::castToElemDDLLikeOptWithData() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithData::getText() const { return "ElemDDLLikeOptWithData"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutNamespace
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutNamespace::~ElemDDLLikeOptWithoutNamespace() {}

// casting
ElemDDLLikeOptWithoutNamespace *ElemDDLLikeOptWithoutNamespace::castToElemDDLLikeOptWithoutNamespace() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutNamespace::getText() const { return "ElemDDLLikeOptWithoutNamespace"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutRegionReplication
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutRegionReplication::~ElemDDLLikeOptWithoutRegionReplication() {}

// casting
ElemDDLLikeOptWithoutRegionReplication *
ElemDDLLikeOptWithoutRegionReplication::castToElemDDLLikeOptWithoutRegionReplication() {
  return this;
}

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutRegionReplication::getText() const {
  return "ElemDDLLikeOptWithoutRegionReplication";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeOptWithoutIncrBackup
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLikeOptWithoutIncrBackup::~ElemDDLLikeOptWithoutIncrBackup() {}

// casting
ElemDDLLikeOptWithoutIncrBackup *ElemDDLLikeOptWithoutIncrBackup::castToElemDDLLikeOptWithoutIncrBackup() {
  return this;
}

//
// methods for tracing
//

const NAString ElemDDLLikeOptWithoutIncrBackup::getText() const { return "ElemDDLLikeOptWithoutIncrBackup"; }
//
// End of File
//
