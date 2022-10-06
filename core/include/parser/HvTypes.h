/* -*-C++-*-
******************************************************************************
*
* File:         HvTypes.h
* Description:  HostVar argument types for Parser
* Language:     C++
*

*
******************************************************************************
*/

#ifndef HVTYPES_H
#define HVTYPES_H

#include "common/Collections.h"
#include "common/NAString.h"
#include "common/NAType.h"

class HVArgType : public NABasicObject {
 public:
  HVArgType(NAString *name, NAType *type) : name_(name), type_(type), useCount_(0), intoCount_(0) {}

  const NAString *getName() const { return name_; }
  const NAType *getType() const { return type_; }
  NAType *getType() { return type_; }
  int &useCount() { return useCount_; }
  int &intoCount() { return intoCount_; }

  // Methods needed for NAKeyLookup collections template

  const NAString *getKey() const { return name_; }

  NABoolean operator==(const HVArgType &other) {
    return *getName() == *other.getName() && *getType() == *other.getType();
  }

 private:
  NAString *name_;
  NAType *type_;
  int useCount_;
  int intoCount_;
};

class HVArgTypeLookup : public NAKeyLookup<NAString, HVArgType> {
 public:
#define HVARGTYPELKP_INIT_SIZE 29
  HVArgTypeLookup(CollHeap *h)
      : NAKeyLookup<NAString, HVArgType>(HVARGTYPELKP_INIT_SIZE, NAKeyLookupEnums::KEY_INSIDE_VALUE, h) {}

  ~HVArgTypeLookup() { clearAndDestroy(); }

 private:
};

#endif  // HVTYPES_H
