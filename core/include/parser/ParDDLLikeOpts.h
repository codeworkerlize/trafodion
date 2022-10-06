#ifndef PARDDLLIKEOPTS_H
#define PARDDLLIKEOPTS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ParDDLLikeOpts.h
 * Description:  base class for derived classes to contain all the
 *               legal options in the LIKE clause of a DDL statement.
 *
 *               Note that the derived classes do not represent parse
 *               nodes.  Several kinds of parse nodes contain these
 *               derived classes.
 *
 *
 * Created:      6/6/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "export/NABasicObject.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ParDDLLikeOpts;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class ParDDLLikeOpts
// -----------------------------------------------------------------------
class ParDDLLikeOpts : public NABasicObject {
 public:
  // types of nodes containing all legal Like options
  // associating with a DDL statement
  enum likeOptsNodeTypeEnum { LIKE_OPTS_ANY_DDL_STMT, LIKE_OPTS_CREATE_COLLATION, LIKE_OPTS_CREATE_TABLE };

  // default constructor
  ParDDLLikeOpts(likeOptsNodeTypeEnum likeOptsNodeType = LIKE_OPTS_ANY_DDL_STMT)
      : likeOptsNodeType_(likeOptsNodeType) {}

  // virtual destructor
  virtual ~ParDDLLikeOpts();

  // assignment
  ParDDLLikeOpts &operator=(const ParDDLLikeOpts &likeOptions);

  // accessors

  likeOptsNodeTypeEnum getLikeOptsNodeType() const { return likeOptsNodeType_; }

 private:
  likeOptsNodeTypeEnum likeOptsNodeType_;

};  // class ParDDLLikeOpts

#endif /* PARDDLLIKEOPTS_H */
