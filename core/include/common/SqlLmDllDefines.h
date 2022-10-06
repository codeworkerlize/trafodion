

#ifndef SQLLMDLL_H
#define SQLLMDLL_H

#include "common/Platform.h"

#ifdef LM_DLL
#define SQLLM_LIB_FUNC __declspec(dllexport)
#else
#define SQLLM_LIB_FUNC __declspec(dllimport)
#endif

#undef SQLLIM_LIB_FUNC
#define SQLLIM_LIB_FUNC

#endif
