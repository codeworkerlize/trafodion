

#ifndef WIDECHAR_H
#define WIDECHAR_H 1

#include "common/NAWinNT.h"

struct _scanfbuf {
  NAWchar *_ptr;
  UInt32 _cnt;
};

typedef struct _scanfbuf SCANBUF;

#define _r _cnt
#define _p _ptr

int na_swscanf(const NAWchar *str, NAWchar const *fmt, ...);

struct _sprintf_buf {
  NAWchar *_ptr;
  int _cnt;
};

typedef struct _sprintf_buf SPRINTF_BUF;

#define _w _cnt

int na_wsprintf(NAWchar *str, NAWchar const *fmt, ...);

#if (defined(NA_C89) || defined(NA_WINNT))
typedef UInt64 u_quad_t;
typedef long quad_t;
#endif

typedef unsigned short u_short;
typedef UInt32 u_int;

#endif
