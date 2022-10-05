
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         UdrAbortCallBack.cpp
 * Description:  abort call back functions from the UDR server
 *
 *
 * Created:      7/08/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "UdrAbortCallBack.h"
#include "UdrFFDC.h"
#include "string.h"

void UdrAbortCallBack::doCallBack(const char *msg, const char *file, UInt32 line) {
#define TEXT_SIZE 1024
  char extMsg[TEXT_SIZE];
  strcpy(extMsg, "MXUDR: ");
  strncat(extMsg, msg, sizeof(extMsg) - strlen(extMsg));
  makeTFDSCall(extMsg, (char *)file, line);
}
