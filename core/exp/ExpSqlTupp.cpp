
/* -*-C++-*-
****************************************************************************
*
* File:         ExpSqlTupp.cpp (previously /executor/sql_tupp.cpp)
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
****************************************************************************
*/

#include "exp/ExpSqlTupp.h"

#include "comexe/ComPackDefs.h"
#include "common/Platform.h"
#include "common/str.h"

tupp::tupp()  // constructor
{
  init();
}

tupp::tupp(const tupp_descriptor *source) {
  init();
  *this = source;  // calls tupp::operator=(const tuppDescriptor *tp)
}

tupp::tupp(const tupp &source) {
  init();
  *this = source;  // calls tupp::operator=(const tupp & source)
}

tupp::~tupp()  // destructor
{
  // release the pointer before deallocating the space for it
  release();
}
Long tupp::pack(void *space) {
  if (tuppDescPointer) {
    tuppDescPointer = (tupp_descriptor *)((Space *)space)->convertToOffset((char *)tuppDescPointer);
  }
  return ((Space *)space)->convertToOffset((char *)this);
}

int tupp::unpack(int base) {
  if (tuppDescPointer) {
    tuppDescPointer = (tupp_descriptor *)CONVERT_TO_PTR(tuppDescPointer, base);
  }

  return 0;
}
tupp_descriptor::tupp_descriptor() { init(); };

#ifdef _DEBUG
void tupp::display() {
  char *dataPointer = getDataPointer();
  int keyLen = getAllocatedSize();

  printBrief(dataPointer, keyLen);
}
#endif
