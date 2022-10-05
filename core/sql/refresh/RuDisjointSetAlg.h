
#ifndef _RU_DISJOINT_SETS_H_
#define _RU_DISJOINT_SETS_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuDisJointSetAlg.h
* Description:  Definition of class CRUDisjointSetAlg
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

//--------------------------------------------------------------------------//
// CRUDisjointSetAlg
//
// This class recieves a graph definition (V,E) and resolove it
// to a disjoint sets,it means that a path from a node in one set to
// any other node in a different set does not exists.
//
//
// A short description of the algorithm :
//		phase 0: We begin in a forest where each node is tree.
//      phase 1: We make a single pass over the edges in E and for each edge
//				 if the left and right nodes belongs to two different trees
//				 we merge the two trees by finding the trees roots and making
//				 one root point to the other
//		phase 2: We go over all the roots and number them by an ever
//				 increasing number
//
//
//	There is a small test function that represent the use of this algorithm.
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUDisjointSetAlg Helper Classes (used to be local classes but
//                                        that caused compiler problems)
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUDisjointSetAlgVertex
//--------------------------------------------------------------------------//

struct CRUDisjointSetAlgVertex {
  CRUDisjointSetAlgVertex(TInt64 id)
      : id_(id),
        rank_(0),
        pParent_(NULL),
        setId_(-1)

            {};

  // A unique Identifier
  TInt64 id_;
  // pointer to the next node in the same set
  CRUDisjointSetAlgVertex *pParent_;
  // maximal depth of nodes that this node is their ancestor
  int rank_;
  // A unique Identifier that represent the set to which this node belongs
  int setId_;
};

//--------------------------------------------------------------------------//
//	CRUDisjointSetAlgEdge
//--------------------------------------------------------------------------//

struct CRUDisjointSetAlgEdge {
  // A default constructor is needed for the Edges list
  CRUDisjointSetAlgEdge(){};

  CRUDisjointSetAlgEdge(TInt64 fromId, TInt64 toId) : leftId_(fromId), rightId(toId){};

  TInt64 leftId_;
  TInt64 rightId;
};

class REFRESH_LIB_CLASS CRUDisjointSetAlg {
 public:
  CRUDisjointSetAlg();

  virtual ~CRUDisjointSetAlg();

 public:
  // returns FALSE if the id was already been added
  void AddVertex(TInt64 id);

  BOOL HasVertex(TInt64 id) {
    CRUDisjointSetAlgVertex *pVertex;
    return V_.Lookup(&id, pVertex);
  }

  void AddEdge(TInt64 first, TInt64 second);

 public:
  void Run();
  int GetNumOfSets() { return sets_.GetCount(); }

  inline int GetNodeSetId(TInt64 id);

 public:
#ifdef _DEBUG
  // This is a test function for this class
  void Test();
#endif

 private:
  void BuildDisjointSetGraph();
  void NameSets();

 private:
  CRUDisjointSetAlgVertex &FindSet(TInt64 id);

  void UnionSets(CRUDisjointSetAlgVertex &first, CRUDisjointSetAlgVertex &second);

 private:
  void RemoveFromRootList(TInt64 id);

 private:
  typedef CDSTInt64Map<CRUDisjointSetAlgVertex *> VertexMap;

  enum { HASH_SIZE = 101 };

 private:
  // A hash table of links to all the tasks
  VertexMap V_;
  CDSList<CRUDisjointSetAlgEdge> E_;

  int numNodes_;

  CDSPtrList<CRUDisjointSetAlgVertex> sets_;
};

//--------------------------------------------------------------------------//
//	CRUDisjointSetAlg Inlines
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUDisjointSetAlg::GetNodeSetId()
//--------------------------------------------------------------------------//

int CRUDisjointSetAlg::GetNodeSetId(TInt64 id) { return FindSet(id).setId_; }

#endif
