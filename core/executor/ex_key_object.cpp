#if 0
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:  
 *               
 *               
 * Created:      7/10/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "ex_key_object.h"

#include "comexe/ComTdb.h"
#include "executor/ex_expr.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "common/str.h"


KeyObject::KeyObject(ex_expr * lkey_expr, ex_expr * hkey_expr,
		     int key_length)
{
}

KeyObject::~KeyObject()
{
}

void KeyObject::position(tupp_descriptor * td)
{
}

short KeyObject::getNextKeys(char * lkey_buffer, short * lkey_excluded,
			     char * hkey_buffer, short * hkey_excluded)
{
  return 0;
}

#endif
