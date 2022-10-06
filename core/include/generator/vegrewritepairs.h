#ifndef VEGREWRITEPAIRS_H
#define VEGREWRITEPAIRS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         vegrewritepairs.h
 * Description:
 *
 *
 * Created:      12/19/96
 * Language:     C++
 * Code location: GenPreCode.C
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "export/NABasicObject.h"
#include "optimizer/ValueDesc.h"

// ---- Class VEGRewritePairs
// This class is a helper class whose claim to life is help
// prevent that a VEGPredicate is rewritten twice during
// the rewrite of predicates in the preCodeGene phase.
// It bahaves as a map of <ValueId, ValueId> pairs
// The left ValueId represents the value of a VEG predicate
// that was rewritten and the right value id is the value
// of the rewritten pred.
// To use this class, check for the existence of the VEGPredicate
// to be rewritten in the left sides of the pairs inside the
// collection. If it is there, don't rewrite but grab the
// right hand side and return that. If it's not there,
// rewrite as usual.

class VEGRewritePairs : public NABasicObject {
 public:
  VEGRewritePairs(CollHeap *heap);

  ~VEGRewritePairs();

  static int valueIdHashFunc(const CollIndex &v);

  // ----------------
  // -- Accesors:
  // ----------------

  // Returns TRUE if original is already in the collection, if so
  // rewritten contains the rewritten id obtained when original
  // was rewritten
  NABoolean getRewritten(ValueId &rewritten, const ValueId &original) const;

  void display() const;
  void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "VEGRewritePairs") const;

  // ----------------
  // --- Mutators:
  // ----------------

  void insert(const ValueId &original, const ValueId &rewritten);

  void clear() {
    CollIndex *key;
    VEGRewritePair *value;
    NAHashDictionaryIterator<CollIndex, VEGRewritePair> iter(vegRewritePairs_);

    for (CollIndex i = 0; i < iter.entries(); i++) {
      iter.getNext(key, value);
      NADELETEBASIC(key, heap_);
      delete value;
    }
    vegRewritePairs_.clear();
  }

  // private:    the oss build fails when this is uncommented

  class VEGRewritePair : public NABasicObject {
   public:
    VEGRewritePair(const ValueId &original, const ValueId &rewritten) : original_(original), rewritten_(rewritten) {}

    // ----------------
    // -- Accesors:
    // ----------------

    const ValueId &getOriginal() const { return original_; }

    const ValueId &getRewritten() const { return rewritten_; }

    void print(FILE *ofd = stdout) const;

    NABoolean operator==(const VEGRewritePair &other) {
      return original_ == other.original_ && rewritten_ == other.rewritten_;
    }

   private:
    ValueId original_,  // predicate before rewrite
        rewritten_;     // predicate after rewrite
  };

 private:
  const VEGRewritePairs::VEGRewritePair *getPair(const ValueId &original) const;

  CollHeap *heap_;
  NAHashDictionary<CollIndex, VEGRewritePair> vegRewritePairs_;

};  // VEGRewritePairs

#endif
// eof
