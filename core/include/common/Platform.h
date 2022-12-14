
#ifndef PLATFORM_H
#define PLATFORM_H

#include <stddef.h>

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#define __STDC_FORMAT_MACROS

/*
// On Linux, either NA_BIG_ENDIAN or NA_LITTLE_ENDIAN may have already
// been set because some other target may have been defined.  The following
// should set it correctly on Linux.
*/
#include <endian.h>
#if __BYTE_ORDER == __LITTLE_ENDIAN
#undef NA_BIG_ENDIAN
#define NA_LITTLE_ENDIAN
#else
#undef NA_LITTLE_ENDIAN
#define NA_BIG_ENDIAN
#endif

/* -----------------------------------------------------------------------  */
/* Set the flavor of Guardian IPC that is used                              */
/* -----------------------------------------------------------------------  */
#define NA_GUARDIAN_IPC

/* ----------------------------------------------------------------------- */
/* MSVC perform template instantiation at compile time,                    */
/* so make sure they see the template implementation files                 */
/* ----------------------------------------------------------------------- */
#define NA_COMPILE_INSTANTIATE

#define NA_MAX_PATH PATH_MAX

/* For declare thread private variables (have to be POD types) */
#define THREAD_P __thread

namespace std {}
using namespace std;

/* For process thread id, it is long on Linux currently */
typedef long ThreadId;

/*
 ---------------------------------------------------------------------
 Used where variable size matters
 ---------------------------------------------------------------------
*/
typedef char Int8;
typedef unsigned char UInt8;
typedef unsigned char UChar;
typedef short Int16;
typedef unsigned short UInt16;

typedef int Int32;
typedef unsigned int UInt32;

typedef float Float32;
typedef double Float64;

typedef long Int64;
typedef unsigned long UInt64;

/* Linux with the gcc compiler */
typedef int TInt32;
typedef long long int TInt64;

/*
 format strings
*/
#define PFLL  "%ld"
#define PFLLX "%lx"
#define PF64  "%ld"
#define PF64X "%lx"
#define PFSZ  "%lu"
#define PFSZX "%lx"

/*
 additional format strings.
 PFV64 and PFLV64 for variable width field and left pad 0s
 PFP64 added for variable precision.
*/
#define PFV64  "%*ld"
#define PFLV64 "%0*ld"
#define PFP64  "%.*ld"

typedef int Lng32;

/* int to replace "unsigned long" or "unsigned long int" */
/* and some will remain UInt32 and others would become UInt64 when done */
typedef unsigned int ULng32;

/* These types are used for variables that must store integers sometime */
/* and pointers other time. Could have given a better name */
typedef long Long;
typedef unsigned long ULong;

// #ifdef _USE_UUID_AS_HBASE_TABLENAME
// #define USE_UUID_AS_HBASE_TABLENAME 1
// #else
// #define USE_UUID_AS_HBASE_TABLENAME 0
// #endif
#define USE_UUID_AS_HBASE_TABLENAME 0

#endif /* PLATFORM_H  */
