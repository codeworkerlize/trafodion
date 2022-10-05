
#ifndef LMJNI_H
#define LMJNI_H

// including jni.h with these symbols doesn't work properly
// save and undefine
#define _ZWIN32     WIN32
#define _Z_WIN32    _WIN32
#define _Z__WIN32__ __WIN32__
#undef WIN32
#undef _WIN32
#undef __WIN32__

#include <jni.h>

// restore
#define WIN32     _ZWIN32
#define _WIN32    _Z_WIN32
#define __WIN32__ _Z__WIN32__

#endif  // LMJNI_H
