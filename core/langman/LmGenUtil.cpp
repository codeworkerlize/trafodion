
/* -*-C++-*-
******************************************************************************
*
* File:         LmGenUtil.cpp
* Description:  LM Utility functions for Generator
*
* Created:      06/18/2001
* Language:     C++
*
*
******************************************************************************
*/
#include "LmGenUtil.h"

#include "langman/LmLangManager.h"
#include "common/str.h"

/* setLMObjectMapping() : Processes Java signature and sets the object mapping
 * in Boolean Array. Clients need to allocate the array of Boolean type for
 * number of params + return type. This function sets object mapping for return
 * type too.
 */
LmResult setLMObjectMapping(const char *routineSig, ComBoolean objMapArray[], ComUInt32 total) {
  const char *sig = routineSig;
  LmResult retCode = LM_OK;
  ComUInt32 pos = 0;

  while (*sig && retCode != LM_ERR) {
    switch (*sig) {
      case '(':
      case '[':
      case ')':
        sig++;
        break;
      case 'V':
      case 'S':
      case 'I':
      case 'J':
      case 'F':
      case 'D':
        objMapArray[pos++] = FALSE;
        sig++;
        break;

      case 'L':
        objMapArray[pos++] = TRUE;
        if ((sig = strchr(sig, ';')) != NULL)
          sig += 1;
        else
          retCode = LM_ERR;
        break;

      default:
        retCode = LM_ERR;

    }  // switch (*sig)
    if (pos == total) break;
  }  // while (*sig)

  return retCode;
}
