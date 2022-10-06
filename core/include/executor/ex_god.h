
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

#ifndef EX_GOD_H
#define EX_GOD_H

#include <limits.h>
#include <stdlib.h>

#include "common/NAHeap.h"
#include "common/Platform.h"

class ExGod : public NABasicObject {
 protected:
  virtual ~ExGod();
};

// the next two methods will eventually be removed after all
// executor objects have been derived from ExGod(or something
// similar).
void *operator new(size_t size, Space *s);
#endif
