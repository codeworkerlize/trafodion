

#ifndef SQLEVENTS_DLL_DEFINES_H
#define SQLEVENTS_DLL_DEFINES_H

#include "common/Platform.h"

#ifndef SQLEVENTS_LIB_FUNC
#ifdef EVENTS_DLL
#define SQLEVENTS_LIB_FUNC __declspec(dllexport)
#else
#define SQLEVENTS_LIB_FUNC __declspec(dllimport)
#endif
#endif  // SQLEVENTS_LIB_FUNC

#undef SQLEVENTS_LIB_FUNC
#define SQLEVENTS_LIB_FUNC

#endif  // SQLEVENTS_DLL_DEFINES.h
