
/* -*-C++-*-
****************************************************************************
*
* File:         ComKeyRange.h
* Description:  defines base classes for objects describing key ranges
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COMKEYRANGE_H
#define COMKEYRANGE_H

#include "executor/ex_expr.h"
#include "exp/ExpCriDesc.h"
#include "export/NAVersionedObject.h"

// forward references
class ex_cri_desc;
class ex_expr;
class keySingleSubsetGen;
class keyMdamGen;

/////////////////////////////////////////////////////////////////////////
//
// Class keyRangeGen
//
// This class contains compiler-generated information used by scan
// operators that can access ordered data directly.  It is a virtual
// class that defines a common interface for different types of key
// access.  The idea is to encapsulate the generation of key ranges.
//
//
/////////////////////////////////////////////////////////////////////////

class keyRangeGen : public NAVersionedObject {
 public:
  enum key_type {
    KEYSINGLESUBSET,  // single key range
    KEYMDAM
  };  // MDAM

  enum { FILLER_LENGTH = 16 };

 protected:
  ExCriDescPtr workCriDesc_;          // 00-07
  UInt32 keyLength_;                  // 08-11
  UInt16 keyValuesAtpIndex_;          // 12-13
  UInt16 excludeFlagAtpIndex_;        // 14-15
  UInt16 dataConvErrorFlagAtpIndex_;  // 16-17

 private:
  Int16 keyType_;  // 18-19

  // while accessing a SQL/MP index, the first 2 bytes of the key value
  // used is the keytag value. This is a 'special' key column that
  // is not part of the base table or the key expressions that are
  // generated. If the keytag_ value is greater than 0, then the
  // first two bytes of the key row are initialized with it in
  // getNextKeyRange() method. The keytag_ value is set by the generator
  // when key info is generated.
  UInt16 keytag_;  // 20-21

  UInt16 flags_;  // 22-23

  char fillersKeyRangeGen_[16];  // 24-39
 public:
  // default constructor needed by UNPACK
  keyRangeGen() : NAVersionedObject(-1) {}

  keyRangeGen(key_type keyType, int keyLen, ex_cri_desc *workCriDesc, unsigned short keyValuesAtpIndex,
              unsigned short excludeFlagAtpIndex, unsigned short dataConvErrorFlagAtpIndex);

  virtual ~keyRangeGen(){};

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual char *findVTblPtr(short classID);

  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(keyRangeGen); }

  virtual keySingleSubsetGen *castToKeySingleSubsetGen() { return NULL; }
  virtual keyMdamGen *castToKeyMdamGen() { return NULL; }

  // for UNPACK
  void fixupVTblPtr();

  // accessor functions
  key_type getType() { return (key_type)keyType_; };

  int getKeyLength() const { return keyLength_; };
  ex_cri_desc *getWorkCriDesc() const { return workCriDesc_; };
  unsigned short getKeyValuesAtpIndex() const { return keyValuesAtpIndex_; };
  unsigned short getExcludeFlagAtpIndex() const { return excludeFlagAtpIndex_; };
  unsigned short getDataConvErrorFlagAtpIndex() const { return dataConvErrorFlagAtpIndex_; };

  UInt16 getKeytag() const { return keytag_; };
  void setKeytag(UInt16 kt) { keytag_ = kt; };

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  virtual ex_expr *getExpressionNode(int pos) { return NULL; }
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for keyRangeGen
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<keyRangeGen> keyRangeGenPtr;

#endif
