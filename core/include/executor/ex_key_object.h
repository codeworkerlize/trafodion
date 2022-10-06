#if 0

#ifndef EX_KEY_OBJECT_H
#define EX_KEY_OBJECT_H


/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * RCS:          $Id: ex_key_object.h,v 1.2 1997/04/23 00:15:54 Exp $
 * Description:
 *
 *
 * Created:      7/10/95
 * Modified:     $Date: 1997/04/23 00:15:54 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *****************************************************************************
 */

class KeyObject : public ExGod
{
  ex_expr * lkeyExpr_;
  ex_expr * hkeyExpr_;
  
  int keyLength_;
  
public:
  KeyObject(ex_expr * lkey_expr, ex_expr * hkey_expr,
	    int key_length);
  ~KeyObject();
  
  // for the given tupp descriptor (data), positions such that
  // later calls to getNextKeys could return all the begin/end
  // (low/high) key pairs.
  void position(tupp_descriptor * td);
  
  // returns 'next' begin and end keys. Keys are encoded.
  // returns -1, if next keys are available. 0, if no more keys.
  short getNextKeys(char * lkey_buffer, short * lkey_excluded,
		    char * hkey_buffer, short * hkey_excluded);
  
  inline int getKeyLength()
    {
      return keyLength_;
    };
  
};

#endif
#endif
