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

#include "ComTdb.h"
#include "ex_expr.h"
#include "ex_stdh.h"
#include "ex_tcb.h"
#include "str.h"


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
