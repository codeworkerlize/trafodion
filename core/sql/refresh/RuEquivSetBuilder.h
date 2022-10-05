
#ifndef _RU_EQUIVSET_BUILDER_H_
#define _RU_EQUIVSET_BUILDER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuEquivSetBuilder.h
* Description:  Definition of class CRUEquivSetBuilder
*
* Created:      11/23/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"
#include "dsplatform.h"
#include "dsptrlist.h"
#include "dsmap.h"
#include "RuMV.h"
#include "RuDisjointSetAlg.h"

//--------------------------------------------------------------------------//
// CRUEquivSetBuilder
//
//	An abstract class for the equivalence sets builders.
//
//	An *equivalence set* is a connected component of objects involved into
//	REFRESH. The connection is implied by the "using" property between MVs
//	and tables, or MVs and MVs.
//
//	There are two kinds of equivalence sets: a table equiv set and an MV
//	equiv set. Each *table equiv set* is a union of non-disjoint table sets
//	used by the involved MVs' expanded trees. Each *MV equiv set* contains
//	involved MVs using each other, either directly or indirectly.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUEquivSetBuilder {
  //----------------------------------//
  //	Public Members
  //----------------------------------//
 public:
  virtual ~CRUEquivSetBuilder() {}

 public:
  // This function is used for entering the input into the builder.
  // The whole graph is built from the given mv's
  virtual void AddMV(CRUMV *pMV) = 0;

  // The main execution function
  virtual void Run() = 0;

  // These functions are used for retrieving the results
  virtual int GetNumOfSets() = 0;

 public:
#ifdef _DEBUG
  // For debug purpose , dumps all sets to the output file
  virtual void DumpSets() = 0;
#endif

 protected:
  CRUDisjointSetAlg &GetDisJointAlg() { return disJointAlg_; }

 private:
  // A helper class used for analyzing the graph into disjoint sets
  CRUDisjointSetAlg disJointAlg_;
};

#endif
