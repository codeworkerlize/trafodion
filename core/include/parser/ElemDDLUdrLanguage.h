
#ifndef ELEMDDLUDRLANGUAGE_H
#define ELEMDDLUDRLANGUAGE_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrLanguage.h
* Description:  class for UDR Language (parse node) elements in
*               DDL statements
*
*
* Created:      10/08/1999
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrLanguage : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrLanguage(ComRoutineLanguage theLanguage);

  // virtual destructor
  virtual ~ElemDDLUdrLanguage(void);

  // cast
  virtual ElemDDLUdrLanguage *castToElemDDLUdrLanguage(void);

  // accessor
  inline ComRoutineLanguage getLanguage(void) const { return language_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineLanguage language_;

};  // class ElemDDLUdrLanguage

#endif /* ELEMDDLUDRLANGUAGE_H */
