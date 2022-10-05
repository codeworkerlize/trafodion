/*-------------------------------------------------------------------------
 *
 * trgm.h
 *    Cost estimate function for bloom indexes.
 *
 * Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    contrib/pg_trgm/trgm.h
 *
 *-------------------------------------------------------------------------
 */


/* -*-C++-*-
******************************************************************************
*
* File:         ngram.h
* Description:  generate ngram string
*
* Created:      2/12/2018
* Language:     C++
*
*
******************************************************************************
*/

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>
#include <assert.h>
#include <wctype.h>
#include "common/Platform.h"

#define VARHDRSZ    ((int)sizeof(int))
#define TRGMHDRSIZE (VARHDRSZ + sizeof(UInt8))
#define GETARR(x)   ((trgm *)((char *)x + TRGMHDRSIZE))

typedef union {
  struct /* Normal varlena (4-byte length) */
  {
    UInt32 va_header;
    char va_data[1];
  } va_4byte;
  struct /* Compressed-in-line format */
  {
    UInt32 va_header;
    UInt32 va_rawsize; /* Original data size (excludes header) */
    char va_data[1];   /* Compressed data */
  } va_compressed;
} varattrib_4b;

#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b *)(PTR))->va_4byte.va_header = (len)&0x3FFFFFFF)

#define VARSIZE_4B(PTR) (((varattrib_4b *)(PTR))->va_4byte.va_header & 0x3FFFFFFF)

#define VARSIZE(PTR) VARSIZE_4B(PTR)

#define ARRNELEM(x)  ((VARSIZE(x) - TRGMHDRSIZE) / sizeof(trgm))
#define NGRAM_LENGTH 3
typedef char trgm[NGRAM_LENGTH];
typedef struct {
  int vl_len_; /* varlena header (do not touch directly!) */
  UInt8 flag;
  char data[1];
} TRGM;
TRGM *generate_trgm(char *str, int slen);
TRGM *generate_wildcard_trgm(const char *str, int slen);
UInt32 trgm2int(trgm *ptr);