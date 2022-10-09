/* -*-C++-*- */
#ifndef ELEMDDLUDFVERSIONTAG_H
#define ELEMDDLUDFVERSIONTAG_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfVersionTag.h
* Description:  class for function version tag (parser node)
*               elements in DDL statements
*
*
* Created:      1/28/2010
* Language:     C++
*
*

*
*
******************************************************************************
*/

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "parser/SqlParserGlobals.h"

class ElemDDLUdfVersionTag : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdfVersionTag(const NAString &theVersionTag, CollHeap *h = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLUdfVersionTag();

  // cast
  virtual ElemDDLUdfVersionTag *castToElemDDLUdfVersionTag(void);

  // accessor
  inline const NAString &getVersionTag(void) const { return versionTag_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NAString versionTag_;

};  // class ElemDDLUdfVersionTag

#endif /* ELEMDDLUDFVERSIONTAG_H */
