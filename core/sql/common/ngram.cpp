/*-------------------------------------------------------------------------
 *
 * trgm_op.c
 *    Cost estimate function for bloom indexes.
 *
 * Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    contrib/pg_trgm/trgm_op.c
 *
 *-------------------------------------------------------------------------
 */

/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
/* -*-C++-*-
******************************************************************************
*
* File:         ngram.cpp
* Description:  generate ngram string
*
* Created:      2/12/2018
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "common/ngram.h"

char *lowerstr_with_len(const char *str, int len);

#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)

#define CMPCHAR(a, b)     (((a) == (b)) ? 0 : (((a) < (b)) ? -1 : 1))
#define CMPPCHAR(a, b, i) CMPCHAR(*(((const char *)(a)) + i), *(((const char *)(b)) + i))

#define CMPTRGM(a, b) \
  (CMPPCHAR(a, b, 0) ? CMPPCHAR(a, b, 0) : (CMPPCHAR(a, b, 1) ? CMPPCHAR(a, b, 1) : CMPPCHAR(a, b, 2)))
#define CPTRGM(a, b)                             \
  do {                                           \
    *(((char *)(a)) + 0) = *(((char *)(b)) + 0); \
    *(((char *)(a)) + 1) = *(((char *)(b)) + 1); \
    *(((char *)(a)) + 2) = *(((char *)(b)) + 2); \
  } while (0);

#define ARRKEY    0x01
#define SIGNKEY   0x02
#define ALLISTRUE 0x04
#define BITBYTE   8
#define SIGLENINT 3 /* >122 => key will toast, so very slow!!! */
#define SIGLEN    (sizeof(int) * SIGLENINT)
#define CALCGTSIZE(flag, len) \
  (TRGMHDRSIZE + (((flag)&ARRKEY) ? ((len) * sizeof(trgm)) : (((flag)&ALLISTRUE) ? 0 : SIGLEN)))

#define LPADDING 0
#define RPADDING 0
#define IGNORECASE
#define pg_database_encoding_max_length() 4

int pg_mblen(const unsigned char *s) {
  int len;

  if ((*s & 0x80) == 0)
    len = 1;
  else if ((*s & 0xe0) == 0xc0)
    len = 2;
  else if ((*s & 0xf0) == 0xe0)
    len = 3;
  else if ((*s & 0xf8) == 0xf0)
    len = 4;
#ifdef NOT_USED
  else if ((*s & 0xfc) == 0xf8)
    len = 5;
  else if ((*s & 0xfe) == 0xfc)
    len = 6;
#endif
  else
    len = 1;
  return len;
}

#define swapcode(TYPE, parmi, parmj, n) \
  do {                                  \
    size_t i = (n) / sizeof(TYPE);      \
    TYPE *pi = (TYPE *)(void *)(parmi); \
    TYPE *pj = (TYPE *)(void *)(parmj); \
    do {                                \
      TYPE t = *pi;                     \
      *pi++ = *pj;                      \
      *pj++ = t;                        \
    } while (--i > 0);                  \
  } while (0)

#define SWAPINIT(a, es) \
  swaptype = ((char *)(a) - (char *)0) % sizeof(long) || (es) % sizeof(long) ? 2 : (es) == sizeof(long) ? 0 : 1;

static void swapfunc(char *a, char *b, size_t n, int swaptype) {
  if (swaptype <= 1)
    swapcode(long, a, b, n);
  else
    swapcode(char, a, b, n);
}

#define swap(a, b)                               \
  if (swaptype == 0) {                           \
    long t = *(long *)(void *)(a);               \
    *(long *)(void *)(a) = *(long *)(void *)(b); \
    *(long *)(void *)(b) = t;                    \
  } else                                         \
    swapfunc((char *)a, b, es, swaptype)

#define vecswap(a, b, n) \
  if ((n) > 0) swapfunc((char *)a, b, n, swaptype)

static char *med3(char *a, char *b, char *c, int (*cmp)(const void *, const void *)) {
  return cmp(a, b) < 0 ? (cmp(b, c) < 0 ? b : (cmp(a, c) < 0 ? c : a)) : (cmp(b, c) > 0 ? b : (cmp(a, c) < 0 ? a : c));
}

#define Min(x, y) ((x) < (y) ? (x) : (y))

void pg_qsort(void *a, size_t n, size_t es, int (*cmp)(const void *, const void *)) {
  char *pa, *pb, *pc, *pd, *pl, *pm, *pn;
  size_t d1, d2;
  int r, swaptype, presorted;

loop:
  SWAPINIT(a, es);
  if (n < 7) {
    for (pm = (char *)a + es; pm < (char *)a + n * es; pm += es)
      for (pl = pm; pl > (char *)a && cmp(pl - es, pl) > 0; pl -= es) swap(pl, pl - es);
    return;
  }
  presorted = 1;
  for (pm = (char *)a + es; pm < (char *)a + n * es; pm += es) {
    if (cmp(pm - es, pm) > 0) {
      presorted = 0;
      break;
    }
  }
  if (presorted) return;
  pm = (char *)a + (n / 2) * es;
  if (n > 7) {
    pl = (char *)a;
    pn = (char *)a + (n - 1) * es;
    if (n > 40) {
      size_t d = (n / 8) * es;

      pl = med3(pl, pl + d, pl + 2 * d, cmp);
      pm = med3(pm - d, pm, pm + d, cmp);
      pn = med3(pn - 2 * d, pn - d, pn, cmp);
    }
    pm = med3(pl, pm, pn, cmp);
  }
  swap(a, pm);
  pa = pb = (char *)a + es;
  pc = pd = (char *)a + (n - 1) * es;
  for (;;) {
    while (pb <= pc && (r = cmp(pb, a)) <= 0) {
      if (r == 0) {
        swap(pa, pb);
        pa += es;
      }
      pb += es;
    }
    while (pb <= pc && (r = cmp(pc, a)) >= 0) {
      if (r == 0) {
        swap(pc, pd);
        pd -= es;
      }
      pc -= es;
    }
    if (pb > pc) break;
    swap(pb, pc);
    pb += es;
    pc -= es;
  }
  pn = (char *)a + n * es;
  d1 = Min(pa - (char *)a, pb - pa);
  vecswap(a, pb - d1, d1);
  d1 = Min(pd - pc, pn - pd - es);
  vecswap(pb, pn - d1, d1);
  d1 = pb - pa;
  d2 = pd - pc;
  if (d1 <= d2) {
    /* Recurse on left partition, then iterate on right partition */
    if (d1 > es) pg_qsort(a, d1 / es, es, cmp);
    if (d2 > es) {
      /* Iterate rather than recurse to save stack space */
      /* pg_qsort(pn - d2, d2 / es, es, cmp); */
      a = pn - d2;
      n = d2 / es;
      goto loop;
    }
  } else {
    /* Recurse on right partition, then iterate on left partition */
    if (d2 > es) pg_qsort(pn - d2, d2 / es, es, cmp);
    if (d1 > es) {
      /* Iterate rather than recurse to save stack space */
      /* pg_qsort(a, d1 / es, es, cmp); */
      n = d1 / es;
      goto loop;
    }
  }
}

#define qsort(a, b, c, d) pg_qsort(a, b, c, d)

typedef UInt32 pg_crc32;
#define INIT_LEGACY_CRC32(crc)            ((crc) = 0xFFFFFFFF)
#define FIN_LEGACY_CRC32(crc)             ((crc) ^= 0xFFFFFFFF)
#define COMP_LEGACY_CRC32(crc, data, len) COMP_CRC32_REFLECTED_TABLE(crc, data, len, pg_crc32_table)

/*
 *  * Sarwate's algorithm, for use with a "reflected" lookup table (but in the
 *   * legacy algorithm, we actually use it on a "normal" table, see above)
 *    */
#define COMP_CRC32_REFLECTED_TABLE(crc, data, len, table)        \
  do {                                                           \
    const unsigned char *__data = (const unsigned char *)(data); \
    UInt32 __len = (len);                                        \
                                                                 \
    while (__len-- > 0) {                                        \
      int __tab_index = ((int)((crc) >> 24) ^ *__data++) & 0xFF; \
      (crc) = table[__tab_index] ^ ((crc) << 8);                 \
    }                                                            \
  } while (0)

/*
 *  * Lookup table for calculating CRC-32 using Sarwate's algorithm.
 *   *
 *    * This table is based on the polynomial
 *     *	x^32+x^26+x^23+x^22+x^16+x^12+x^11+x^10+x^8+x^7+x^5+x^4+x^2+x+1.
 *      * (This is the same polynomial used in Ethernet checksums, for instance.)
 *       * Using Williams' terms, this is the "normal", not "reflected" version.
 *        */
const UInt32 pg_crc32_table[256] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3, 0x0EDB8832,
    0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91, 0x1DB71064, 0x6AB020F2,
    0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7, 0x136C9856, 0x646BA8C0, 0xFD62F97A,
    0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
    0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3,
    0x45DF5C75, 0xDCD60DCF, 0xABD13D59, 0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423,
    0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB,
    0xB6662D3D, 0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01, 0x6B6B51F4,
    0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950,
    0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65, 0x4DB26158, 0x3AB551CE, 0xA3BC0074,
    0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
    0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9, 0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525,
    0x206F85B3, 0xB966D409, 0xCE61E49F, 0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81,
    0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615,
    0x73DC1683, 0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7, 0xFED41B76,
    0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8, 0xA1D1937E,
    0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B, 0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6,
    0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
    0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7,
    0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D, 0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F,
    0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7,
    0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45, 0xA00AE278,
    0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC,
    0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9, 0xBDBDF21C, 0xCABAC28A, 0x53B39330,
    0x24B4A3A6, 0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
    0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D};

/*
 * Finds first word in string, returns pointer to the word,
 * endword points to the character after word
 */
static char *find_word(char *str, int lenstr, char **endword, int *charlen) {
  char *beginword = str;

  if (beginword - str >= lenstr) return NULL;

  *endword = beginword;
  *charlen = 0;
  while (*endword - str < lenstr) {
    *endword += pg_mblen((const unsigned char *)*endword);
    (*charlen)++;
  }

  return beginword;
}

/*
 *  Reduce a trigram (three possibly multi-byte characters) to a trgm,
 *  which is always exactly three bytes.  If we have three single-byte
 *  characters, we just use them as-is; otherwise we form a hash value.
 */
void compact_trigram(trgm *tptr, char *str, int bytelen) {
  if (bytelen == 3) {
    CPTRGM(tptr, str);
  } else {
    pg_crc32 crc;

    INIT_LEGACY_CRC32(crc);
    COMP_LEGACY_CRC32(crc, str, bytelen);
    FIN_LEGACY_CRC32(crc);

    /*
     * use only 3 upper bytes from crc, hope, it's good enough hashing
     */
    CPTRGM(tptr, &crc);
  }
}

/*
 * Adds trigrams from words (already padded).
 */
static trgm *make_trigrams(trgm *tptr, char *str, int bytelen, int charlen) {
  char *ptr = str;

  if (charlen < 3) return tptr;

  if (bytelen > charlen) {
    // Find multibyte character boundaries and apply compact_trigram
    int lenfirst = pg_mblen((const unsigned char *)str), lenmiddle = pg_mblen((const unsigned char *)(str + lenfirst)),
        lenlast = pg_mblen((const unsigned char *)(str + lenfirst + lenmiddle));

    while ((ptr - str) + lenfirst + lenmiddle + lenlast <= bytelen) {
      compact_trigram(tptr, ptr, lenfirst + lenmiddle + lenlast);

      ptr += lenfirst;
      tptr++;

      lenfirst = lenmiddle;
      lenmiddle = lenlast;
      lenlast = pg_mblen((const unsigned char *)(ptr + lenfirst + lenmiddle));
    }
  } else

  {
    while (ptr - str < bytelen - 2 /* number of trigrams = strlen - 2 */) {
      CPTRGM(tptr, ptr);
      ptr++;
      tptr++;
    }
  }

  return tptr;
}

/*
 * Make array of trigrams without sorting and removing duplicate items.
 *
 * trg: where to return the array of trigrams.
 * str: source string, of length slen bytes.
 *
 * Returns length of the generated array.
 */
static int generate_trgm_only(trgm *trg, char *str, int slen) {
  trgm *tptr;
  char *buf;
  int charlen, bytelen;
  char *bword, *eword;

  if (slen + LPADDING + RPADDING < 3 || slen == 0) return 0;

  tptr = trg;

  /* Allocate a buffer for case-folded, blank-padded words */
  buf = (char *)malloc(slen * pg_database_encoding_max_length() + 4);

  if (LPADDING > 0) {
    *buf = ' ';
    if (LPADDING > 1) *(buf + 1) = ' ';
  }

  eword = str;
  while ((bword = find_word(eword, slen - (eword - str), &eword, &charlen)) != NULL) {
#ifdef IGNORECASE
    bword = lowerstr_with_len(bword, eword - bword);
    bytelen = strlen(bword);
#else
    bytelen = eword - bword;
#endif

    memcpy(buf + LPADDING, bword, bytelen);

#ifdef IGNORECASE
    free(bword);
#endif

    buf[LPADDING + bytelen] = ' ';
    buf[LPADDING + bytelen + 1] = ' ';

    /*
     * count trigrams
     */
    tptr = make_trigrams(tptr, buf, bytelen + LPADDING + RPADDING, charlen + LPADDING + RPADDING);
  }

  free(buf);

  return tptr - trg;
}

static int comp_trgm(const void *a, const void *b) { return CMPTRGM(a, b); }

static int unique_array(trgm *a, int len) {
  trgm *curend, *tmp;

  curend = tmp = a;
  while (tmp - a < len)
    if (CMPTRGM(tmp, curend)) {
      curend++;
      CPTRGM(curend, tmp);
      tmp++;
    } else
      tmp++;

  return curend + 1 - a;
}

/*
 * Make array of trigrams with sorting and removing duplicate items.
 *
 * str: source string, of length slen bytes.
 *
 * Returns the sorted array of unique trigrams.
 */
TRGM *generate_trgm(char *str, int slen) {
  TRGM *trg;
  int len;
  trg = (TRGM *)malloc(TRGMHDRSIZE + sizeof(trgm) * (slen / 2 + 1) * 3);
  trg->flag = ARRKEY;

  len = generate_trgm_only(GETARR(trg), str, slen);
  SET_VARSIZE(trg, CALCGTSIZE(ARRKEY, len));

  if (len == 0) return trg;

  /*
   * Make trigrams unique.
   */
  if (len > 1) {
    qsort((void *)GETARR(trg), len, sizeof(trgm), comp_trgm);
    len = unique_array(GETARR(trg), len);
  }

  SET_VARSIZE(trg, CALCGTSIZE(ARRKEY, len));

  return trg;
}

#define ISWORDCHR(c) (t_isalpha(c) || t_isdigit(c))
#define ISPRINTABLECHAR(a) \
  (isascii(*(unsigned char *)(a)) && (isalnum(*(unsigned char *)(a)) || *(unsigned char *)(a) == ' '))
#define TOUCHAR(x)   (*((const unsigned char *)(x)))
#define t_isdigit(x) isdigit(TOUCHAR(x))
#define t_isspace(x) isspace(TOUCHAR(x))
#define t_isprint(x) isprint(TOUCHAR(x))
#define t_iseq(x, c) (TOUCHAR(x) == (unsigned char)(c))

#define COPYCHAR(d, s)  (*((unsigned char *)(d)) = TOUCHAR(s))
#define ISESCAPECHAR(x) (*(x) == '\\') /* Wildcard escape character */
#define ISWILDCARDCHAR(x)                  \
  (*(x) == '_' || *(x) == '%') /* Wildcard \
                                * meta-character */
typedef unsigned int Oid;
#define DEFAULT_COLLATION_OID 100
#define InvalidOid            ((Oid)0)
#define OidIsValid(objectId)  ((bool)((objectId) != InvalidOid))
#define HAVE_LOCALE_T         1

bool lc_ctype_is_c(Oid collation) {
  /*
   * If we're asked about "collation 0", return false, so that the code will
   * go into the non-C path and report that the collation is bogus.
   */
  if (!OidIsValid(collation)) return false;

  /*
   * If we're asked about the default collation, we have to inquire of the C
   * library.  Cache the result so we only have to compute it once.
   */
  if (collation == DEFAULT_COLLATION_OID) {
    static int result = -1;
    char *localeptr;

    if (result >= 0) return (bool)result;
    localeptr = setlocale(LC_CTYPE, NULL);
    if (!localeptr) printf("invalid LC_CTYPE setting\n");

    if (strcmp(localeptr, "C") == 0)
      result = true;
    else if (strcmp(localeptr, "POSIX") == 0)
      result = true;
    else
      result = false;
    return (bool)result;
  }

  return true;
}

struct pg_locale_struct {
  char provider;
  union {
#ifdef HAVE_LOCALE_T
    locale_t lt;
#endif
    int dummy; /* in case we have neither LOCALE_T nor ICU */
  } info;
};

typedef struct pg_locale_struct *pg_locale_t;

#define COLLPROVIDER_DEFAULT 'd'
#define COLLPROVIDER_ICU     'i'
#define COLLPROVIDER_LIBC    'c'

/*
 * char2wchar --- convert multibyte characters to wide characters
 *
 * This has almost the API of mbstowcs_l(), except that *from need not be
 * null-terminated; instead, the number of input bytes is specified as
 * fromlen.  Also, we ereport() rather than returning -1 for invalid
 * input encoding.  tolen is the maximum number of wchar_t's to store at *to.
 * The output will be zero-terminated iff there is room.
 */

size_t char2wchar(wchar_t *to, size_t tolen, const char *from, size_t fromlen, pg_locale_t locale) {
  size_t result;

  assert(!locale || locale->provider == COLLPROVIDER_LIBC);

  if (tolen == 0) return 0;

  char str[fromlen + 1];
  memcpy(str, from, fromlen);
  str[fromlen] = '\0';
  result = mbstowcs(to, str, tolen);
  return result;
}

int t_isalpha(const char *ptr) {
  int clen = pg_mblen((const unsigned char *)ptr);
  wchar_t character[2];
  Oid collation = DEFAULT_COLLATION_OID; /* TODO */
  pg_locale_t mylocale = 0;              /* TODO */

  if (clen == 1 || lc_ctype_is_c(collation)) return isalpha(TOUCHAR(ptr));

  char2wchar(character, 2, ptr, clen, mylocale);

  return iswalpha((wint_t)character[0]);
}
//============end ISWORDCHR============

/*
 * lowerstr_with_len --- fold string to lower case
 *
 * Input string need not be null-terminated.
 *
 * Returned string is palloc'd
 */
char *lowerstr_with_len(const char *str, int len) {
  char *out;

  if (len == 0) return strdup("");

  const char *ptr = str;
  char *outptr;

  outptr = out = (char *)malloc(sizeof(char) * (len + 1));
  while ((ptr - str) < len && *ptr) {
    *outptr++ = tolower(TOUCHAR(ptr));
    ptr++;
  }
  *outptr = '\0';

  return out;
}

/*
 * Extract the next non-wildcard part of a search string, i.e. a word bounded
 * by '_' or '%' meta-characters, non-word characters or string end.
 *
 * str: source string, of length lenstr bytes (need not be null-terminated)
 * buf: where to return the substring (must be long enough)
 * *bytelen: receives byte length of the found substring
 * *charlen: receives character length of the found substring
 *
 * Returns pointer to end+1 of the found substring in the source string.
 * Returns NULL if no word found (in which case buf, bytelen, charlen not set)
 *
 * If the found word is bounded by non-word characters or string boundaries
 * then this function will include corresponding padding spaces into buf.
 */
static const char *get_wildcard_part(const char *str, int lenstr, char *buf, int *bytelen, int *charlen) {
  const char *beginword = str;
  const char *endword;
  char *s = buf;
  bool in_leading_wildcard_meta = false;
  bool in_trailing_wildcard_meta = false;
  bool in_escape = false;
  int clen;

  /*
   * Find the first word character, remembering whether preceding character
   * was wildcard meta-character.  Note that the in_escape state persists
   * from this loop to the next one, since we may exit at a word character
   * that is in_escape.
   */
  while (beginword - str < lenstr) {
    if (in_escape) {
      if (ISWORDCHR(beginword)) break;
      in_escape = false;
      in_leading_wildcard_meta = false;
    } else {
      if (ISESCAPECHAR(beginword))
        in_escape = true;
      else if (ISWILDCARDCHAR(beginword))
        in_leading_wildcard_meta = true;
      else if (ISWORDCHR(beginword))
        break;
      else
        in_leading_wildcard_meta = false;
    }
    beginword += pg_mblen((const unsigned char *)beginword);
  }

  /*
   * Handle string end.
   */
  if (beginword - str >= lenstr) return NULL;

  /*
   * Add left padding spaces if preceding character wasn't wildcard
   * meta-character.
   */
  *charlen = 0;
  if (!in_leading_wildcard_meta) {
    if (LPADDING > 0) {
      *s++ = ' ';
      (*charlen)++;
      if (LPADDING > 1) {
        *s++ = ' ';
        (*charlen)++;
      }
    }
  }

  /*
   * Copy data into buf until wildcard meta-character, non-word character or
   * string boundary.  Strip escapes during copy.
   */
  endword = beginword;
  while (endword - str < lenstr) {
    clen = pg_mblen((const unsigned char *)endword);
    if (in_escape) {
      if (ISWORDCHR(endword)) {
        memcpy(s, endword, clen);
        (*charlen)++;
        s += clen;
      } else {
        /*
         * Back up endword to the escape character when stopping at an
         * escaped char, so that subsequent get_wildcard_part will
         * restart from the escape character.  We assume here that
         * escape chars are single-byte.
         */
        endword--;
        break;
      }
      in_escape = false;
    } else {
      if (ISESCAPECHAR(endword))
        in_escape = true;
      else if (ISWILDCARDCHAR(endword)) {
        in_trailing_wildcard_meta = true;
        break;
      } else if (ISWORDCHR(endword)) {
        memcpy(s, endword, clen);
        (*charlen)++;
        s += clen;
      } else
        break;
    }
    endword += clen;
  }

  /*
   * Add right padding spaces if next character isn't wildcard
   * meta-character.
   */
  if (!in_trailing_wildcard_meta) {
    if (RPADDING > 0) {
      *s++ = ' ';
      (*charlen)++;
      if (RPADDING > 1) {
        *s++ = ' ';
        (*charlen)++;
      }
    }
  }

  *bytelen = s - buf;
  return endword;
}

/*
 * Generates trigrams for wildcard search string.
 *
 * Returns array of trigrams that must occur in any string that matches the
 * wildcard string.  For example, given pattern "a%bcd%" the trigrams
 * " a", "bcd" would be extracted.
 */
TRGM *generate_wildcard_trgm(const char *str, int slen) {
  TRGM *trg;
  char *buf, *buf2;
  trgm *tptr;
  int len, charlen, bytelen;
  const char *eword;

  // protect_out_of_mem(slen);

  trg = (TRGM *)malloc(TRGMHDRSIZE + sizeof(trgm) * (slen / 2 + 1) * 3);
  trg->flag = ARRKEY;
  SET_VARSIZE(trg, TRGMHDRSIZE);

  if (slen + LPADDING + RPADDING < 3 || slen == 0) return trg;

  tptr = GETARR(trg);

  /* Allocate a buffer for blank-padded, but not yet case-folded, words */
  buf = (char *)malloc(sizeof(char) * (slen + 4));

  /*
   * Extract trigrams from each substring extracted by get_wildcard_part.
   */
  eword = str;
  while ((eword = get_wildcard_part(eword, slen - (eword - str), buf, &bytelen, &charlen)) != NULL) {
#ifdef IGNORECASE
    buf2 = lowerstr_with_len(buf, bytelen);
    bytelen = strlen(buf2);
#else
    buf2 = buf;
#endif

    /*
     * count trigrams
     */
    tptr = make_trigrams(tptr, buf2, bytelen, charlen);

#ifdef IGNORECASE
    free(buf2);
#endif
  }

  free(buf);

  if ((len = tptr - GETARR(trg)) == 0) return trg;

  /*
   * Make trigrams unique.
   */
  if (len > 1) {
    qsort((void *)GETARR(trg), len, sizeof(trgm), comp_trgm);
    len = unique_array(GETARR(trg), len);
  }

  SET_VARSIZE(trg, CALCGTSIZE(ARRKEY, len));

  return trg;
}
