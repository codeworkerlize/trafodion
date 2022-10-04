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
// Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:  Number format for TO_CHAR
 *
 * Created:      2/21/2019
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include <math.h>
#include <unistd.h>
#include <zlib.h>
#include <stdlib.h>
#include <cstdlib>
#include <cmath>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <regex.h>
#include <uuid/uuid.h>
#include <time.h>
#include "exp_numberformat.h"
#include "exp/exp_clause_derived.h"

#define FORMAT_STATE_END       1
#define FORMAT_STATE_PROCESSED 2
#define FORMAT_STATE_CHAR      3
#define FORMAT_NODE_MAX_SIZE   64  // max number format node

#define IS_DECIMAL(_f)  ((_f)->flag & NUM_F_DECIMAL)        // NUM_DEC
#define IS_LDECIMAL(_f) ((_f)->flag & NUM_F_LDECIMAL)       // NUM_D
#define IS_ZERO(_f)     ((_f)->flag & NUM_F_ZERO)           // NUM_0
#define IS_BLANK(_f)    ((_f)->flag & NUM_F_BLANK)          // NUM_B
#define IS_FILLMODE(_f) ((_f)->flag & NUM_F_FILLMODE)       // NUM_FM
#define IS_BRACKET(_f)  ((_f)->flag & NUM_F_BRACKET)        // NUM_PR
#define IS_MINUS(_f)    ((_f)->flag & NUM_F_MINUS)          // NUM_MI
#define IS_LSIGN(_f)    ((_f)->flag & NUM_F_LSIGN)          // NUM_S
#define IS_ROMAN(_f)    ((_f)->flag & NUM_F_ROMAN)          // NUM_RN
#define IS_MULTI(_f)    ((_f)->flag & NUM_F_MULTI)          // NUM_V
#define IS_EEEE(_f)     ((_f)->flag & NUM_F_EEEE)           // NUM_E
#define IS_CURRENCY(_f) ((_f)->flag & NUM_F_CURRENCY_POST)  // NUM_C

#define NUM_F_DECIMAL       (1 << 1)
#define NUM_F_LDECIMAL      (1 << 2)
#define NUM_F_ZERO          (1 << 3)
#define NUM_F_BLANK         (1 << 4)
#define NUM_F_FILLMODE      (1 << 5)
#define NUM_F_LSIGN         (1 << 6)
#define NUM_F_BRACKET       (1 << 7)
#define NUM_F_MINUS         (1 << 8)
#define NUM_F_PLUS          (1 << 9)
#define NUM_F_ROMAN         (1 << 10)
#define NUM_F_MULTI         (1 << 11)
#define NUM_F_MINUS_POST    (1 << 12)
#define NUM_F_EEEE          (1 << 13)
#define NUM_F_CURRENCY_POST (1 << 14)

#define NUM_LSIGN_PRE  (-1)
#define NUM_LSIGN_POST 1
#define NUM_LSIGN_NONE 0

#define IS_PREDEC_SPACE(_n) \
  (IS_ZERO((_n)->Num) == FALSE && (_n)->number == (_n)->number_p && *(_n)->number == '0' && (_n)->Num->post != 0)

#define MAXFLOATWIDTH  60
#define MAXDOUBLEWIDTH 200
/* ----------
 * Roman numbers
 * ----------
 */
static const char *const rm1[] = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", NULL};
static const char *const rm10[] = {"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC", NULL};
static const char *const rm100[] = {"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM", NULL};

#define MAXCURRENCYSYMBOL 4
typedef struct {
  int pre,                  /* (count) numbers before decimal */
      post,                 /* (count) numbers after decimal  */
      lsign,                /* want locales sign              */
      flag,                 /* number parameters              */
      pre_lsign_num,        /* tmp value for lsign            */
      multi,                /* multiplier for 'V'             */
      zero_start,           /* position of first zero         */
      zero_end,             /* position of last zero          */
      need_locale,          /* needs it locale                */
      need_currency_symbol; /* need currency symbol   */
  char currency_symbol[MAXCURRENCYSYMBOL];
} NUMDesc;

typedef struct {
  const char *name;
  int len;
  int id;
  bool is_digit;
} NumFmtKeyWord;

struct FormatNode {
  int type;                 /* node type               */
  const NumFmtKeyWord *key; /* if node type is KEYWORD	*/
  char character;           /* if node type is CHAR    */
  int suffix;               /* keyword suffix          */
};

typedef struct {
  FormatNode format[FORMAT_NODE_MAX_SIZE + 1];
  char str[FORMAT_NODE_MAX_SIZE + 1];
  bool valid;
  int age;
  NUMDesc Num;
} NUMEntry;

typedef struct NUMProc {
  NUMDesc *Num; /* number description		*/

  int sign,           /* '-' or '+'			*/
      sign_wrote,     /* was sign write		*/
      num_count,      /* number of write digits	*/
      num_in,         /* is inside number		*/
      num_curr,       /* current position in number	*/
      out_pre_spaces; /* spaces before first digit	*/

  char *number,       /* string with number	*/
      *number_p,      /* pointer to current number position */
      *inout,         /* in / out buffer	*/
      *inout_p,       /* pointer to current inout position */
      *last_relevant; /* last relevant number after decimal point */

  char L_negative_sign, /* Locale */
      L_positive_sign, decimal, L_thousands_sep, L_currency_symbol[MAXCURRENCYSYMBOL];
} NUMProc;

#define NUM_FMT_IDX_SIZE ('~' - ' ')

static const NumFmtKeyWord NUM_FMT_KEY[] = {
    /*	name,   len,   id			                         */
    {",", 1, ExpNumerFormat::NUM_COMMA}, /* , */
    {".", 1, ExpNumerFormat::NUM_DEC},   /* . */
    {"0", 1, ExpNumerFormat::NUM_0},     /* 0 */
    {"9", 1, ExpNumerFormat::NUM_9},     /* 9 */
    {"B", 1, ExpNumerFormat::NUM_B},     /* B */
    {"C", 1, ExpNumerFormat::NUM_C},     /* C */
    {"D", 1, ExpNumerFormat::NUM_D},     /* D */
    {"EEEE", 4, ExpNumerFormat::NUM_E},  /* E */
    {"FM", 2, ExpNumerFormat::NUM_FM},   /* F */
    {"G", 1, ExpNumerFormat::NUM_G},     /* G */
    {"L", 1, ExpNumerFormat::NUM_L},     /* L */
    {"MI", 2, ExpNumerFormat::NUM_MI},   /* M */
    {"PR", 2, ExpNumerFormat::NUM_PR},   /* <>*/
    {"RN", 2, ExpNumerFormat::NUM_RN},   /* R */
    {"Rn", 2, ExpNumerFormat::NUM_RN},
    {"S", 1, ExpNumerFormat::NUM_S},
    {"V", 1, ExpNumerFormat::NUM_V},    /* V */
    {"b", 1, ExpNumerFormat::NUM_b},    /* b */
    {"c", 1, ExpNumerFormat::NUM_C},    /* c */
    {"d", 1, ExpNumerFormat::NUM_d},    /* d */
    {"eeee", 4, ExpNumerFormat::NUM_E}, /* e */
    {"fm", 2, ExpNumerFormat::NUM_FM},  /* f */
    {"g", 1, ExpNumerFormat::NUM_G},    /* g */
    {"l", 1, ExpNumerFormat::NUM_L},    /* l */
    {"mi", 2, ExpNumerFormat::NUM_MI},  /* m */
    {"pr", 2, ExpNumerFormat::NUM_PR},
    {"rn", 2, ExpNumerFormat::NUM_rn}, /* r */
    {"rN", 2, ExpNumerFormat::NUM_rn},
    {"s", 1, ExpNumerFormat::NUM_S},
    {"v", 1, ExpNumerFormat::NUM_V}, /* v */
    /* last */
    {NULL, 0, 0}};

static const int NUM_FMT_IDX[NUM_FMT_IDX_SIZE] = {
    /*====== start from ascii 32(space) ======*/
    -1,                        /*space*/
    -1,                        /* 	!  */
    -1,                        /*  "  */
    -1,                        /*  #  */
    -1,                        /*  $  */
    -1,                        /*  %  */
    -1,                        /*  &  */
    -1,                        /*  '  */
    -1,                        /*  (  */
    -1,                        /*  )  */
    -1,                        /*  *  */
    -1,                        /*  +  */
    ExpNumerFormat::NUM_COMMA, /*  ,  */
    -1,                        /*  -  */
    ExpNumerFormat::NUM_DEC,   /*  .  */
    -1,                        /*  /  */
    ExpNumerFormat::NUM_0,     /*  0  */
    -1,                        /*  1  */
    -1,                        /*  2  */
    -1,                        /*  3  */
    -1,                        /*  4  */
    -1,                        /*  5  */
    -1,                        /*  6  */
    -1,                        /*  7  */
    -1,                        /*  8  */
    ExpNumerFormat::NUM_9,     /*  9  */
    -1,                        /*  :  */
    -1,                        /*  ;  */
    -1,                        /*  <  */
    -1,                        /*  =  */
    -1,                        /*  >  */
    -1,                        /*  ?  */
    -1,                        /*  @  */
    -1,                        /*  A  */
    ExpNumerFormat::NUM_B,     /*  B  */
    ExpNumerFormat::NUM_C,     /*  C  */
    ExpNumerFormat::NUM_D,     /*  D  */
    ExpNumerFormat::NUM_E,     /*  E  */
    ExpNumerFormat::NUM_FM,    /*  F  */
    ExpNumerFormat::NUM_G,     /*  G  */
    -1,                        /*  H  */
    -1,                        /*  I  */
    -1,                        /*  J  */
    -1,                        /*  K  */
    ExpNumerFormat::NUM_L,     /*  L  */
    ExpNumerFormat::NUM_MI,    /*  M  */
    -1,                        /*  N  */
    -1,                        /*  O  */
    ExpNumerFormat::NUM_PR,    /*  P  */
    -1,                        /*  Q  */
    ExpNumerFormat::NUM_RN,    /*  R  */
    ExpNumerFormat::NUM_S,     /*  S  */
    -1,                        /*  T  */
    -1,                        /*  U  */
    ExpNumerFormat::NUM_V,     /*  V  */
    -1,                        /*  W  */
    -1,                        /*  X  */
    -1,                        /*  Y  */
    -1,                        /*  Z  */
    -1,                        /*  [  */
    -1,                        /*  \  */
    -1,                        /*  ]  */
    -1,                        /*  ^  */
    -1,                        /*  _  */
    -1,                        /*  `  */
    -1,                        /*  a  */
    ExpNumerFormat::NUM_b,     /*  b  */
    ExpNumerFormat::NUM_c,     /*  c  */
    ExpNumerFormat::NUM_d,     /*  d  */
    ExpNumerFormat::NUM_e,     /*  e  */
    ExpNumerFormat::NUM_fm,    /*  f  */
    ExpNumerFormat::NUM_g,     /*  g  */
    -1,                        /*  h  */
    -1,                        /*  i  */
    -1,                        /*  j  */
    -1,                        /*  k  */
    ExpNumerFormat::NUM_l,     /*  l  */
    ExpNumerFormat::NUM_mi,    /*  m  */
    -1,                        /*  n  */
    -1,                        /*  o  */
    ExpNumerFormat::NUM_pr,    /*  p  */
    -1,                        /*  q  */
    ExpNumerFormat::NUM_rn,    /*  r  */
    ExpNumerFormat::NUM_s,     /*  s  */
    -1,                        /*  t  */
    -1,                        /*  u  */
    ExpNumerFormat::NUM_v,     /*  v  */
    -1,                        /*  w  */
    -1,                        /*  x  */
    -1,                        /*  y  */
    -1,                        /*  z  */
    -1,                        /*  {  */
    -1,                        /*  |  */
    -1,                        /*  }  */

    /*====== to ascii 125('}') end ======*/
};

static char *fill_str(char *str, int c, int max) {
  memset(str, c, max);
  *(str + max) = '\0';
  return str;
}

static Lng32 int_to_roman(int number, char *result) {
  int len = 0, num = 0;
  char *p = NULL, numstr[5];

  if (number > 3999 || number < 1) {
    fill_str(result, '#', 15);
    return -1;
  }
  len = snprintf(numstr, sizeof(numstr), "%d", number);

  for (p = numstr; *p != '\0'; p++, --len) {
    num = *p - 49; /* 48 ascii + 1 */
    if (num < 0) continue;

    if (len > 3) {
      while (num-- != -1) strcat(result, "M");
    } else {
      if (len == 3)
        strcat(result, rm100[num]);
      else if (len == 2)
        strcat(result, rm10[num]);
      else if (len == 1)
        strcat(result, rm1[num]);
    }
  }
  return 0;
}

static Lng32 NUMDesc_prepare(NUMDesc *num, FormatNode *n, FormatNode *pFirst) {
  if (n->type != FORMAT_STATE_PROCESSED) return -1;

  if (IS_EEEE(num) && ExpNumerFormat::NUM_E != n->key->id) return -1;  //("EEEE" must be the last pattern used)

  if (ExpNumerFormat::NUM_MI != n->key->id && IS_MINUS(num))
    return -1;  //("MI" can appear only in the last position of a number format)

  if (ExpNumerFormat::NUM_PR != n->key->id && IS_BRACKET(num))
    return -1;  //("PR" can appear only in the last position of a number format)

  if (ExpNumerFormat::NUM_S != n->key->id && IS_LSIGN(num) && ExpNumerFormat::NUM_S != pFirst->key->id)
    return -1;  //("S" can appear only in the first or last position of a number format)

  if (ExpNumerFormat::NUM_C != n->key->id && IS_CURRENCY(num) &&
      (ExpNumerFormat::NUM_C != pFirst->key->id && ExpNumerFormat::NUM_S != pFirst->key->id) &&
      (ExpNumerFormat::NUM_S != n->key->id && ExpNumerFormat::NUM_MI != n->key->id &&
       ExpNumerFormat::NUM_PR != n->key->id))
    return -1;  //("C" can appear only in the first or last position of a number format or just ahead of NUM_S or NUM_MI
                //orNUM_PR )

  switch (n->key->id) {
    case ExpNumerFormat::NUM_9: {
      if (IS_MULTI(num)) {
        ++num->multi;
        break;
      }
      if (IS_DECIMAL(num))
        ++num->post;
      else
        ++num->pre;
    } break;
    case ExpNumerFormat::NUM_0: {
      if (!IS_ZERO(num) && !IS_DECIMAL(num)) {
        num->flag |= NUM_F_ZERO;
        num->zero_start = num->pre + 1;
      }
      if (!IS_DECIMAL(num))
        ++num->pre;
      else
        ++num->post;

      num->zero_end = num->pre + num->post;
    } break;
    case ExpNumerFormat::NUM_B:
      if (num->pre == 0 && num->post == 0 && (!IS_ZERO(num))) num->flag |= NUM_F_BLANK;
      break;
    case ExpNumerFormat::NUM_D:
    case ExpNumerFormat::NUM_d:
      num->flag |= NUM_F_LDECIMAL;
      num->need_locale = TRUE;
      /*Don't break*/
    case ExpNumerFormat::NUM_DEC: {
      if (IS_DECIMAL(num)) return -1;  //(multiple decimal points)
      if (IS_MULTI(num)) return -1;    //(cannot use "V" and decimal point together)
      num->flag |= NUM_F_DECIMAL;
    } break;
    case ExpNumerFormat::NUM_FM:
      num->flag |= NUM_F_FILLMODE;
      break;
    case ExpNumerFormat::NUM_S: {
      if (IS_LSIGN(num)) return -1;                     //(cannot use "S" twice);
      if (IS_MINUS(num) || IS_BRACKET(num)) return -1;  //(cannot use "S" and "MI"\"PR" together)
      if (!IS_DECIMAL(num)) {
        num->lsign = NUM_LSIGN_PRE;
        num->pre_lsign_num = num->pre;
        num->need_locale = TRUE;
        num->flag |= NUM_F_LSIGN;
      } else if (num->lsign == NUM_LSIGN_NONE) {
        num->lsign = NUM_LSIGN_POST;
        num->need_locale = TRUE;
        num->flag |= NUM_F_LSIGN;
      }
    } break;
    case ExpNumerFormat::NUM_MI: {
      if (IS_LSIGN(num)) return -1;  //(cannot use "S" and "MI" together)
      num->flag |= NUM_F_MINUS;
      if (IS_DECIMAL(num)) num->flag |= NUM_F_MINUS_POST;
    } break;
    case ExpNumerFormat::NUM_PR: {
      if (IS_LSIGN(num) || IS_MINUS(num)) return -1;  //(cannot use "PR" and "S"/"MI" together)
      num->flag |= NUM_F_BRACKET;
    } break;
    case ExpNumerFormat::NUM_rn:
    case ExpNumerFormat::NUM_RN:
      num->flag |= NUM_F_ROMAN;
      break;
    case ExpNumerFormat::NUM_L: {
      // Not support locale currency symbol yet
      // Since we need to provide a better way to allow user
      // to set locale symbol.
      return -1;
      if (num->need_currency_symbol) return -1;  //(cannot use "L" again)
      num->need_currency_symbol = TRUE;
      num->need_locale = TRUE;
    } break;
    case ExpNumerFormat::NUM_C: {
      if (num->need_currency_symbol) return -1;
      // find number, so NUM_C is not in the first position
      if (num->pre > 0 || num->post > 0)
        num->flag |= NUM_F_CURRENCY_POST;
      else
        n->type = FORMAT_STATE_CHAR;
      num->need_currency_symbol = TRUE;
      strcpy(num->currency_symbol, "CNY");
    } break;
    case ExpNumerFormat::NUM_COMMA:
    case ExpNumerFormat::NUM_G: {
      if (pFirst)  // check previous one
      {
        if (n == pFirst)  // comma cannot begin a number format
          return -1;
        // comma cannot appear to the right of a decimal character or period in a number format
        if (IS_DECIMAL(num) || IS_LDECIMAL(num)) return -1;
      }
      if (ExpNumerFormat::NUM_G == n->key->id) num->need_locale = TRUE;
    } break;
    case ExpNumerFormat::NUM_V: {
      if (IS_DECIMAL(num)) return -1;  //(cannot use "V" and decimal point together)
      num->flag |= NUM_F_MULTI;
    } break;
    case ExpNumerFormat::NUM_E: {
      if (IS_EEEE(num)) return -1;  //(cannot use "EEEE" twice)
      if (IS_BLANK(num) || IS_FILLMODE(num) || IS_LSIGN(num) || IS_BRACKET(num) || IS_MINUS(num) || IS_ROMAN(num) ||
          IS_MULTI(num))
        return -1;  //("EEEE" may only be used together with digit and decimal point patterns);
      num->flag |= NUM_F_EEEE;
    } break;
  }
  return 0;
}

static const NumFmtKeyWord *index_seq_search(const char *str, const NumFmtKeyWord *kw, const int *index) {
  int poz;
  if (*str <= ' ' || *str >= '~') return NULL;

  if ((poz = *(index + (*str - ' '))) > -1) {
    const NumFmtKeyWord *k = kw + poz;
    do {
      if (strncmp(str, k->name, k->len) == 0) return k;
      k++;
      if (!k->name) return NULL;
    } while (*str == *k->name);
  }
  return NULL;
}

static Lng32 parse_format(FormatNode *node, const char *str, const NumFmtKeyWord *kw, const int *index, NUMDesc *Num,
                          CollHeap *heap, ComDiagsArea **diagsArea) {
  FormatNode *n;
  int node_set = 0, suffix, last = 0;
  n = node;

  while (*str) {
    suffix = 0;
    if (*str && (n->key = index_seq_search(str, kw, index)) != NULL) {
      n->type = FORMAT_STATE_PROCESSED;
      n->suffix = 0;
      node_set = 1;
      if (n->key->len) str += n->key->len;

      if (NUMDesc_prepare(Num, n, node)) {
        ExRaiseSqlError(heap, diagsArea, EXE_TOCHAR_INVALID_FORMAT_ERROR);
        return -1;
      }
    } else if (*str) {
      if (*str == '"' && last != '\\') {
        int x = 0;
        while (*(++str)) {
          if (*str == '"' && x != '\\') {
            str++;
            break;
          } else if (*str == '\\' && x != '\\') {
            x = '\\';
            continue;
          }
          n->type = FORMAT_STATE_CHAR;
          n->character = *str;
          n->key = NULL;
          n->suffix = 0;
          ++n;
          x = *str;
        }
        node_set = 0;
        suffix = 0;
        last = 0;
      } else if (*str == '\\' && last != '\\' && *(str + 1) == '"') {
        last = *str;
        str++;
      } else if (*str == '$') {
        if (Num->need_currency_symbol) {
          ExRaiseSqlError(heap, diagsArea, EXE_TOCHAR_INVALID_FORMAT_ERROR);
          **diagsArea << DgString0("TO_CHAR");
          return -1;
        }
        Num->need_currency_symbol = TRUE;
        strcpy(Num->currency_symbol, "$");
        n->type = FORMAT_STATE_CHAR;
        n->character = *str;
        n->key = NULL;
        node_set = 1;
        last = 0;
        str++;
      } else {
        ExRaiseSqlError(heap, diagsArea, EXE_TOCHAR_INVALID_FORMAT_ERROR);
        **diagsArea << DgString0("TO_CHAR");
        return -1;
      }
    }
    /* end */
    if (node_set) {
      if (n->type == FORMAT_STATE_PROCESSED) n->suffix = suffix;
      ++n;

      n->suffix = 0;
      node_set = 0;
    }
  }
  n->type = FORMAT_STATE_END;
  n->suffix = 0;
  return 0;
}

static NUMEntry *NUM_fetch(const char *str, CollHeap *heap, ComDiagsArea **diagsArea) {
  NUMEntry *ent = new (heap) NUMEntry();
  ent->Num.flag = 0;
  ent->Num.lsign = 0;
  ent->Num.pre = 0;
  ent->Num.post = 0;
  ent->Num.pre_lsign_num = 0;
  ent->Num.need_locale = 0;
  ent->Num.need_currency_symbol = 0;
  memset(ent->Num.currency_symbol, 0, MAXCURRENCYSYMBOL);
  ent->Num.multi = 0;
  ent->Num.zero_start = 0;
  ent->Num.zero_end = 0;

  if (parse_format(ent->format, str, NUM_FMT_KEY, NUM_FMT_IDX, &ent->Num, heap, diagsArea)) return NULL;
  ent->valid = true;
  return ent;
}

static char *get_last_relevant_decnum(char *num) {
  char *result, *p = strchr(num, '.');

  if (!p) return NULL;

  result = p;

  while (*(++p)) {
    if (*p != '0') result = p;
  }

  return result;
}

static void NUM_numpart_to_char(NUMProc *Np, int id) {
  int end;

  if (IS_ROMAN(Np->Num)) return;

  /* Note: in this elog() output not set '\0' in 'inout' */

  Np->num_in = FALSE;

  /*
   * Write sign if real number will write to output Note: IS_PREDEC_SPACE()
   * handle "9.9" --> " .1"
   */
  if (Np->sign_wrote == FALSE &&
      (Np->num_curr >= Np->out_pre_spaces || (IS_ZERO(Np->Num) && Np->Num->zero_start == Np->num_curr)) &&
      (IS_PREDEC_SPACE(Np) == FALSE || (Np->last_relevant && *Np->last_relevant == '.'))) {
    if (IS_LSIGN(Np->Num)) {
      if (Np->Num->lsign == NUM_LSIGN_PRE) {
        if (Np->sign == '-')
          *Np->inout_p = Np->L_negative_sign;
        else
          *Np->inout_p = Np->L_positive_sign;
        Np->inout_p += strlen(Np->inout_p);
        Np->sign_wrote = TRUE;
      }
    } else if (IS_BRACKET(Np->Num)) {
      *Np->inout_p = Np->sign == '+' ? ' ' : '<';
      ++Np->inout_p;
      Np->sign_wrote = TRUE;
    } else if (Np->sign == '+') {
      if (!IS_FILLMODE(Np->Num)) {
        *Np->inout_p = ' '; /* Write + */
        ++Np->inout_p;
      }
      Np->sign_wrote = TRUE;
    } else if (Np->sign == '-') { /* Write - */
      *Np->inout_p = '-';
      ++Np->inout_p;
      Np->sign_wrote = TRUE;
    }
    // add currency symbol
    if (Np->sign_wrote || (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_POST)) {
      if (Np->Num->need_currency_symbol && !IS_CURRENCY(Np->Num)) {
        strncpy(Np->inout_p, Np->L_currency_symbol, MAXCURRENCYSYMBOL);
        Np->inout_p += strlen(Np->inout_p);
        Np->Num->need_currency_symbol = FALSE;
      }
    }
  }
  if ((IS_MINUS(Np->Num) || IS_FILLMODE(Np->Num)) && Np->num_curr >= Np->out_pre_spaces &&
      IS_PREDEC_SPACE(Np) == FALSE) {
    if (Np->Num->need_currency_symbol && !IS_CURRENCY(Np->Num)) {
      strncpy(Np->inout_p, Np->L_currency_symbol, MAXCURRENCYSYMBOL);
      Np->inout_p += strlen(Np->inout_p);
      Np->Num->need_currency_symbol = FALSE;
    }
  }
  /*
   * digits / FM / Zero / Dec. point
   */
  if (id == ExpNumerFormat::NUM_9 || id == ExpNumerFormat::NUM_0 || id == ExpNumerFormat::NUM_D ||
      id == ExpNumerFormat::NUM_d || id == ExpNumerFormat::NUM_DEC) {
    if (Np->num_curr < Np->out_pre_spaces && (Np->Num->zero_start > Np->num_curr || !IS_ZERO(Np->Num))) {
      /*
       * Write blank space
       */
      if (!IS_FILLMODE(Np->Num)) {
        *Np->inout_p = ' '; /* Write ' ' */
        ++Np->inout_p;
      }
    } else if (IS_ZERO(Np->Num) && Np->num_curr < Np->out_pre_spaces && Np->Num->zero_start <= Np->num_curr) {
      /*
       * Write ZERO
       */
      *Np->inout_p = '0'; /* Write '0' */
      ++Np->inout_p;
      Np->num_in = TRUE;
    } else {
      /*
       * Write Decimal point
       */
      if (*Np->number_p == '.') {
        if (!Np->last_relevant || *Np->last_relevant != '.') {
          *Np->inout_p = Np->decimal; /* Write DEC/D */
          Np->inout_p += strlen(Np->inout_p);
        }

        /*
         * Ora 'n' -- FM9.9 --> 'n.'
         */
        else if (IS_FILLMODE(Np->Num) && Np->last_relevant && *Np->last_relevant == '.') {
          *Np->inout_p = Np->decimal; /* Write DEC/D */
          Np->inout_p += strlen(Np->inout_p);
        }
      } else {
        /*
         * Write Digits
         */
        if (Np->last_relevant && Np->number_p > Np->last_relevant && id != ExpNumerFormat::NUM_0)
          ;

        /*
         * '0.1' -- 9.9 --> '  .1'
         */
        else if (IS_PREDEC_SPACE(Np)) {
          if (!IS_FILLMODE(Np->Num)) {
            *Np->inout_p = ' ';
            ++Np->inout_p;
          }

          /*
           * '0' -- FM9.9 --> '0.'
           */
          else if (Np->last_relevant && *Np->last_relevant == '.') {
            *Np->inout_p = '0';
            ++Np->inout_p;
          }
        } else {
          *Np->inout_p = *Np->number_p; /* Write DIGIT */
          ++Np->inout_p;
          Np->num_in = TRUE;
        }
      }
      /* do no exceed string length */
      if (*Np->number_p) ++Np->number_p;
    }

    end = Np->num_count + (Np->out_pre_spaces ? 1 : 0) + (IS_DECIMAL(Np->Num) ? 1 : 0);

    if (Np->last_relevant && Np->last_relevant == Np->number_p) end = Np->num_curr;

    if (Np->num_curr + 1 == end) {
      if (Np->sign_wrote == TRUE && IS_BRACKET(Np->Num)) {
        if (IS_CURRENCY(Np->Num) && Np->Num->need_currency_symbol && Np->L_currency_symbol) {
          strncpy(Np->inout_p, Np->L_currency_symbol, MAXCURRENCYSYMBOL);
          Np->inout_p += strlen(Np->inout_p);
          Np->Num->need_currency_symbol = FALSE;
        }
        *Np->inout_p = Np->sign == '+' ? ' ' : '>';
        ++Np->inout_p;
      } else if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_POST) {
        if (IS_CURRENCY(Np->Num) && Np->Num->need_currency_symbol && Np->L_currency_symbol) {
          strncpy(Np->inout_p, Np->L_currency_symbol, MAXCURRENCYSYMBOL);
          Np->inout_p += strlen(Np->inout_p);
          Np->Num->need_currency_symbol = FALSE;
        }
        if (Np->sign == '-')
          *Np->inout_p = Np->L_negative_sign;
        else
          *Np->inout_p = Np->L_positive_sign;
        Np->inout_p += strlen(Np->inout_p);
      }
    }
  }
  ++Np->num_curr;
}

static char *NUM_processor(FormatNode *node, NUMDesc *Num, char *inout, char *number, int to_char_out_pre_spaces,
                           int sign) {
  FormatNode *n;
  NUMProc _Np;
  NUMProc *Np = &_Np;

  memset(Np, 0, sizeof(NUMProc));
  Np->Num = Num;
  Np->number = number;
  Np->inout = inout;
  Np->last_relevant = NULL;
  if (Np->Num->zero_start) --Np->Num->zero_start;

  if (IS_EEEE(Np->Num)) {
    return strcpy(inout, number);
  }

  /*
   * Roman correction
   */
  if (IS_ROMAN(Np->Num)) {
    Np->Num->lsign = Np->Num->pre_lsign_num = Np->Num->post = Np->Num->pre = Np->out_pre_spaces = Np->sign = 0;

    if (IS_FILLMODE(Np->Num)) {
      Np->Num->flag = 0;
      Np->Num->flag |= NUM_F_FILLMODE;
    } else
      Np->Num->flag = 0;
    Np->Num->flag |= NUM_F_ROMAN;
  }

  /*
   * Sign
   */
  Np->sign = sign;

  /* MI - write sign itself and not in number */
  if (IS_MINUS(Np->Num)) {
    Np->sign_wrote = TRUE; /* needn't sign */
  } else {
    if (Np->sign != '-') {
      if (IS_BRACKET(Np->Num) && IS_FILLMODE(Np->Num)) Np->Num->flag &= ~NUM_F_BRACKET;
      if (IS_MINUS(Np->Num)) Np->Num->flag &= ~NUM_F_MINUS;
    }

    if (Np->sign == '+' && IS_FILLMODE(Np->Num) && IS_LSIGN(Np->Num) == FALSE)
      Np->sign_wrote = TRUE; /* needn't sign */
    else
      Np->sign_wrote = FALSE; /* need sign */

    if (Np->Num->lsign == NUM_LSIGN_PRE && Np->Num->pre == Np->Num->pre_lsign_num) Np->Num->lsign = NUM_LSIGN_POST;
  }

  /*
   * Count
   */
  Np->num_count = Np->Num->post + Np->Num->pre - 1;

  Np->out_pre_spaces = to_char_out_pre_spaces;

  if (IS_FILLMODE(Np->Num) && IS_DECIMAL(Np->Num)) {
    Np->last_relevant = get_last_relevant_decnum(Np->number);

    /*
     * If any '0' specifiers are present, make sure we don't strip
     * those digits.
     */
    if (Np->last_relevant && Np->Num->zero_end > Np->out_pre_spaces) {
      char *last_zero;

      last_zero = Np->number + (Np->Num->zero_end - Np->out_pre_spaces);
      if (Np->last_relevant < last_zero) Np->last_relevant = last_zero;
    }
  }

  if (Np->sign_wrote == FALSE && Np->out_pre_spaces == 0) ++Np->num_count;

  Np->num_in = 0;
  Np->num_curr = 0;

  /*
   * Locale
   */
  Np->L_negative_sign = '-';
  Np->L_positive_sign = '+';
  Np->decimal = '.';
  Np->L_thousands_sep = ',';
  if (Num->need_currency_symbol && Num->currency_symbol)
    strncpy(Np->L_currency_symbol, Num->currency_symbol, MAXCURRENCYSYMBOL);
  else
    strcpy(Np->L_currency_symbol, "¥");

  /*
   * Processor direct cycle
   */

  Np->number_p = Np->number;

  for (n = node, Np->inout_p = Np->inout; n->type != FORMAT_STATE_END; n++) {
    /*
     * Format pictures actions
     */
    if (n->type == FORMAT_STATE_PROCESSED) {
      /*
       * Create/reading digit/zero/blank/sing
       *
       * 'NUM_S' note: The locale sign is anchored to number and we
       * read/write it when we work with first or last number
       * (NUM_0/NUM_9). This is reason why NUM_S missing in follow
       * switch().
       */
      switch (n->key->id) {
        case ExpNumerFormat::NUM_9:
        case ExpNumerFormat::NUM_0:
        case ExpNumerFormat::NUM_DEC:
        case ExpNumerFormat::NUM_D:
        case ExpNumerFormat::NUM_d:
          NUM_numpart_to_char(Np, n->key->id);
          continue; /* for() */
        // add currency symbol (NUM_C)
        case ExpNumerFormat::NUM_C: {
          if (IS_CURRENCY(Np->Num) && Np->Num->need_currency_symbol && Np->L_currency_symbol) {
            strncpy(Np->inout_p, Np->L_currency_symbol, MAXCURRENCYSYMBOL);
            Np->inout_p += strlen(Np->inout_p);
            Np->Num->need_currency_symbol = FALSE;
          }
        }
          continue;
        case ExpNumerFormat::NUM_COMMA:
          if (!Np->num_in) {
            if (IS_FILLMODE(Np->Num))
              continue;
            else
              *Np->inout_p = ' ';
          } else
            *Np->inout_p = ',';
          break;
        case ExpNumerFormat::NUM_G:
          if (!Np->num_in) {
            if (IS_FILLMODE(Np->Num))
              continue;
            else {
              int x = sizeof(char);  // strlen(&Np->L_thousands_sep);
              memset(Np->inout_p, ' ', x);
              Np->inout_p += x - 1;
            }
          } else {
            *Np->inout_p = Np->L_thousands_sep;
            Np->inout_p += strlen(Np->inout_p) - 1;
          }
          break;
        case ExpNumerFormat::NUM_RN:
          if (IS_FILLMODE(Np->Num)) {
            strcpy(Np->inout_p, Np->number_p);
            Np->inout_p += strlen(Np->inout_p) - 1;
          } else {
            sprintf(Np->inout_p, "%15s", Np->number_p);
            Np->inout_p += strlen(Np->inout_p) - 1;
          }
          break;
        case ExpNumerFormat::NUM_rn: {
          NAString s(Np->number_p);
          s.toLower();
          if (IS_FILLMODE(Np->Num)) {
            strcpy(Np->inout_p, s.data());
            Np->inout_p += strlen(Np->inout_p) - 1;
          } else {
            sprintf(Np->inout_p, "%15s", s.data());
            Np->inout_p += strlen(Np->inout_p) - 1;
          }
        } break;
        case ExpNumerFormat::NUM_MI: {
          if (Np->sign == '-')
            *Np->inout_p = '-';
          else if (IS_FILLMODE(Np->Num))
            continue;
          else
            *Np->inout_p = ' ';
        } break;
        default:
          continue;
          break;
      }
      Np->inout_p++;
    }
    /*else
      {
        if (n->type == FORMAT_STATE_CHAR && Np->Num->need_currency_symbol)
          strcpy(Np->L_currency_symbol,&n->character);
      }*/
  }

  *Np->inout_p = '\0';
  return Np->inout;
}

static Int64 lcl_atoi64(const char *s) {
  Int64 n = 0;
  char *t = (char *)s;
  char c;

  while (*t != 0) {
    c = *t++;
    if (c < '0' || c > '9') continue;
    n = n * 10 + c - '0';
  }
  if (*s == '-') n = -n;
  return n;
}

static void assignNumDesc(NUMDesc &Num, const NUMEntry *ent) {
  Num.flag = ent->Num.flag;
  Num.lsign = ent->Num.lsign;
  Num.pre = ent->Num.pre;
  Num.post = ent->Num.post;
  Num.pre_lsign_num = ent->Num.pre_lsign_num;
  Num.need_locale = ent->Num.need_locale;
  Num.need_currency_symbol = ent->Num.need_currency_symbol;
  strncpy(Num.currency_symbol, ent->Num.currency_symbol, MAXCURRENCYSYMBOL);
  Num.multi = ent->Num.multi;
  Num.zero_start = ent->Num.zero_start;
  Num.zero_end = ent->Num.zero_end;
}

static Lng32 getFomatNodeCount(const FormatNode *format) {
  Lng32 n = 0;
  if (!format) return 0;

  for (int i = 1; i <= FORMAT_NODE_MAX_SIZE && format->type != FORMAT_STATE_END; ++i) {
    format++;
    ++n;
  }
  return n;
}

Lng32 ExpNumerFormat::convertBigNumToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1,
                                          Attributes *arg2, char *arg1Str, char *arg2Str, CollHeap *heap,
                                          ComDiagsArea **diagsArea) {
  Lng32 arg2len = arg2->getLength();
  char *numfmtStr = new (heap) char[arg2len + 1];
  str_cpy_all(numfmtStr, arg2Str, arg2len);
  numfmtStr[arg2len] = 0;

  int sign = 0;
  FormatNode *format = NULL;
  NUMDesc Num;
  NUMEntry *ent = NUM_fetch(numfmtStr, heap, diagsArea);
  if (!ent) {
    NADELETEBASIC(numfmtStr, (heap));
    return -1;
  }

  format = ent->format;
  assignNumDesc(Num, ent);

  int out_pre_spaces = 0;
  char *numstr = new (heap) char[arg2len + 1];
  memset(numstr, 0, arg2len + 1);
  char *p = NULL;

  if (IS_ROMAN(&Num)) {
    ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
    **diagsArea << DgString0("TO_CHAR");
    NADELETEBASIC(numstr, (heap));
    NADELETEBASIC(numfmtStr, (heap));
    return -1;
  } else if (IS_EEEE(&Num)) {
    ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
    **diagsArea << DgString0("TO_CHAR");
    NADELETEBASIC(numstr, (heap));
    NADELETEBASIC(numfmtStr, (heap));
    return -1;
  } else {
    if (IS_MULTI(&Num)) {
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      **diagsArea << DgString0("TO_CHAR");
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return -1;
    }

    if (dataValue[0] == '-')
      sign = '-';
    else
      sign = '+';

    Lng32 nRealScale = 0;
    char *pfindStr = strstr(dataValue, ".");
    if (pfindStr) nRealScale = strlen(dataValue) - (pfindStr - dataValue) - 1;
    if (nRealScale > Num.post) {
      Int32 nBigNumLength = arg1->getLength();
      // Extract sign
      char leftSign = BIGN_GET_SIGN(arg1Str, nBigNumLength);
      // Clear sign bits
      BIGN_CLR_SIGN(arg1Str, nBigNumLength);

      char tmp_bignum[nBigNumLength];
      memset(tmp_bignum, 0, nBigNumLength);
      // round bignum
      // e.g.
      // select to_char(123456789012345678901234.56789,'999999999999999999999999.999')  ==> 123456789012345678901234.568
      short result =
          BigNumHelper::RoundHelper(nBigNumLength, nBigNumLength, arg1Str, arg1->getScale() - Num.post, tmp_bignum);
      if (result < 0) {
        if (heap)
          ExRaiseSqlError(heap, diagsArea, EXE_NUMERIC_OVERFLOW, NULL, NULL, NULL, NULL,
                          " The error occurred when rounding up a value.");
        NADELETEBASIC(numstr, (heap));
        NADELETEBASIC(numfmtStr, (heap));
        return -1;
      }

      char tmp[arg2len + 1];
      memset(tmp, 0, arg2len + 1);
      short intermediateLen;
      if (::convDoIt(tmp_bignum, arg1->getLength(), arg1->getDatatype(), arg1->getPrecision(), arg1->getScale(), tmp,
                     arg2len, REC_BYTE_V_ASCII, 0, 0, (char *)&intermediateLen, sizeof(short), heap,
                     diagsArea) != ex_expr::EXPR_OK) {
        NADELETEBASIC(numstr, (heap));
        NADELETEBASIC(numfmtStr, (heap));
        return ex_expr::EXPR_ERROR;
      }
      tmp[intermediateLen] = 0;
      // handle '0.236' --> '.24', '1.236' -->'1.24'
      short nCpyOffset = 0;
      if (tmp[0] == '0' && Num.post > 0) nCpyOffset = 1;
      Lng32 nCpyLen = strlen(tmp) - nCpyOffset;
      str_cpy_all(numstr, tmp + nCpyOffset, nCpyLen);
      // Complement '0'
      // e.g. to_char(23.99,'99.9')  expand 24 to 24.0
      pfindStr = strstr(numstr, ".");
      if (pfindStr)
        nRealScale = strlen(numstr) - (pfindStr - numstr) - 1;
      else
        nRealScale = 0;
      // After BigNumHelper::RoundHelper, If there are NO decimal digits,
      // we should add decimal point, and then complement '0'.
      // If there are decimal digits, we just complement '0'.
      if (Num.post > nRealScale && nRealScale == 0) {
        fill_str(numstr + nCpyLen, '.', 1);
        fill_str(numstr + nCpyLen + 1, '0', Num.post);
      } else
        fill_str(numstr + nCpyLen, '0', Num.post - nRealScale);

      // Reset sign bits
      if (leftSign) BIGN_SET_SIGN(arg1Str, nBigNumLength);
    } else {
      // handle '-.2' --> '0.2'  '-0.2'  --> '0.2'
      Lng32 nCpyOffset = 0;
      if (*dataValue == '-') {
        nCpyOffset = 1;
        if (dataValue[1] == '.') numstr[0] = '0';
      }
      Lng32 nCpyLen = strlen(dataValue) - nCpyOffset;
      str_cpy_all(numstr, dataValue + nCpyOffset, nCpyLen);

      // Complement '0'
      // e.g. to_char(1.34,'9.999')  expand 1.34 to 1.340
      if (Num.post > nRealScale && nRealScale == 0) {
        fill_str(numstr + nCpyLen, '.', 1);
        fill_str(numstr + nCpyLen + 1, '0', Num.post);
      } else
        fill_str(numstr + nCpyLen, '0', Num.post - nRealScale);
    }

    int numstr_pre_len = strlen(numstr);
    if ((p = strchr(numstr, '.'))) numstr_pre_len = p - numstr;

    /* needs padding? */
    if (numstr_pre_len < Num.pre) out_pre_spaces = Num.pre - numstr_pre_len;
    /* overflowed prefix digit format? */
    else if (numstr_pre_len > Num.pre) {
      Lng32 nFormatCnt = getFomatNodeCount(format) + 1;
      nFormatCnt = min(arg0->getLength(), nFormatCnt);
      fill_str(result, '#', nFormatCnt);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
    if (IS_BLANK(&Num) && strlen(numstr) == 1 &&
        *numstr == '0') {  // e.g. TO_CHAR(0.123,'B999') ---> '    '(four spaces)
      Lng32 nBlank = getFomatNodeCount(format);
      nBlank = min(arg0->getLength(), nBlank);
      fill_str(result, ' ', nBlank);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
  }

  NUM_processor(format, &Num, result, numstr, out_pre_spaces, sign);
  NADELETEBASIC(numstr, (heap));
  NADELETEBASIC(numfmtStr, (heap));
  return 0;
}

Lng32 ExpNumerFormat::convertFloatToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1,
                                         Attributes *arg2, char *arg1Str, char *arg2Str, CollHeap *heap,
                                         ComDiagsArea **diagsArea) {
  Lng32 arg2len = arg2->getLength();
  char *numfmtStr = new (heap) char[arg2len + 1];
  str_cpy_all(numfmtStr, arg2Str, arg2len);
  numfmtStr[arg2len] = 0;

  int sign = 0;
  FormatNode *format = NULL;
  NUMDesc Num;
  NUMEntry *ent = NUM_fetch(numfmtStr, heap, diagsArea);
  if (!ent) {
    NADELETEBASIC(numfmtStr, (heap));
    return -1;
  }

  format = ent->format;
  assignNumDesc(Num, ent);

  int out_pre_spaces = 0;
  char *numstr = new (heap) char[MAXDOUBLEWIDTH];
  memset(numstr, 0, MAXDOUBLEWIDTH);
  // char *orgnum = NULL;
  double fValue = atof(dataValue);
  char *p = NULL;

  if (IS_ROMAN(&Num)) {
    Lng32 tmpSource;
    Lng32 dataConversionErrorFlag = 0;
    if (::convDoIt(arg1Str, arg1->getLength(), arg1->getDatatype(), arg1->getPrecision(), arg1->getScale(),
                   (char *)&tmpSource, sizeof(Lng32), REC_BIN32_SIGNED, 0, 0, 0, 0, heap, diagsArea,
                   ConvInstruction::CONV_UNKNOWN, &dataConversionErrorFlag) != ex_expr::EXPR_OK) {
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      **diagsArea << DgString0("TO_CHAR");
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return -1;
    }
    if (dataConversionErrorFlag) {
      fill_str(numstr, '#', 15);
    } else {
      float ftmp = float(tmpSource) / float(pow(10, arg1->getScale()));
      ftmp = round(ftmp);
      tmpSource = Lng32(ftmp);
      if (tmpSource > 3999) {
        fill_str(numstr, '#', 15);
      } else {
        int_to_roman((Int32)tmpSource, numstr);
      }
    }
  } else if (IS_EEEE(&Num)) {
    snprintf(numstr, MAXDOUBLEWIDTH, "%+.*E", Num.post, fValue);
    /*
     * Swap a leading positive sign for a space.
     */
    if (*numstr == '+') *numstr = ' ';
  } else {
    double val = fValue;

    if (IS_MULTI(&Num)) {
      double multi = pow((double)10, (double)Num.multi);
      val = fValue * multi;
      Num.pre += Num.multi;
    }

    if (fValue < 0.0)
      sign = '-';
    else
      sign = '+';
    Lng32 nRealScale = 0;
    char *pfindStr = strstr(dataValue, ".");
    if (pfindStr) nRealScale = strlen(dataValue) - (pfindStr - dataValue) - 1;
    if (nRealScale > Num.post) {
      // handle '0.236' --> '.24', '1.236' -->'1.24'
      char tmp[MAXDOUBLEWIDTH];
      snprintf(tmp, MAXDOUBLEWIDTH, "%.*f", Num.post, fabs(val));
      short nCpyOffset = 0;
      if ((1 - fabs(val)) > 0 && Num.post > 0) nCpyOffset = 1;
      str_cpy_all(numstr, tmp + nCpyOffset, strlen(tmp) - nCpyOffset);
    } else {
      // handle '-.2' --> '0.2'  '-0.2'  --> '0.2'
      Lng32 nCpyOffset = 0;
      if (*dataValue == '-') {
        nCpyOffset = 1;
        if (dataValue[1] == '.') numstr[0] = '0';
      }
      Lng32 nCpyLen = strlen(dataValue) - nCpyOffset;
      str_cpy_all(numstr, dataValue + nCpyOffset, nCpyLen);

      // Complement '0'
      // e.g. to_char(1.34,'9.999')  expand 1.34 to 1.340
      if (Num.post > nRealScale && nRealScale == 0) {
        fill_str(numstr + nCpyLen, '.', 1);
        fill_str(numstr + nCpyLen + 1, '0', Num.post);
      } else
        fill_str(numstr + nCpyLen, '0', Num.post - nRealScale);
    }

    int numstr_pre_len = strlen(numstr);
    if ((p = strchr(numstr, '.'))) numstr_pre_len = p - numstr;

    /* needs padding? */
    if (numstr_pre_len < Num.pre) out_pre_spaces = Num.pre - numstr_pre_len;
    /* overflowed prefix digit format? */
    else if (numstr_pre_len > Num.pre) {
      Lng32 nFormatCnt = getFomatNodeCount(format) + 1;
      nFormatCnt = min(arg0->getLength(), nFormatCnt);
      fill_str(result, '#', nFormatCnt);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
    if (IS_BLANK(&Num) && strlen(numstr) == 1 &&
        *numstr == '0') {  // e.g. TO_CHAR(0.123,'B999') ---> '    '(four spaces)
      Lng32 nBlank = getFomatNodeCount(format);
      nBlank = min(arg0->getLength(), nBlank);
      fill_str(result, ' ', nBlank);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
  }

  NUM_processor(format, &Num, result, numstr, out_pre_spaces, sign);
  NADELETEBASIC(numstr, (heap));
  NADELETEBASIC(numfmtStr, (heap));
  return 0;
}

Lng32 ExpNumerFormat::convertInt32ToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1,
                                         Attributes *arg2, char *arg1Str, char *arg2Str, CollHeap *heap,
                                         ComDiagsArea **diagsArea) {
  Lng32 arg2len = arg2->getLength();
  char *numfmtStr = new (heap) char[arg2len + 1];
  str_cpy_all(numfmtStr, arg2Str, arg2len);
  numfmtStr[arg2len] = 0;

  int sign = 0;
  FormatNode *format = NULL;
  NUMDesc Num;
  NUMEntry *ent = NUM_fetch(numfmtStr, heap, diagsArea);
  if (!ent) {
    NADELETEBASIC(numfmtStr, (heap));
    return -1;
  }

  format = ent->format;
  assignNumDesc(Num, ent);

  int out_pre_spaces = 0;
  char *numstr = new (heap) char[MAXDOUBLEWIDTH];
  memset(numstr, 0, MAXDOUBLEWIDTH);
  // char *orgnum = NULL;
  Int32 nValue = atoi(dataValue);
  if (IS_ROMAN(&Num)) {
    int_to_roman(nValue, numstr);
  } else if (IS_EEEE(&Num)) {
    /* we can do it easily because float8 won't lose any precision */
    double val = (double)nValue;
    snprintf(numstr, MAXDOUBLEWIDTH, "%+.*e", Num.post, val);
    /*
     * Swap a leading positive sign for a space.
     */
    if (*numstr == '+') *numstr = ' ';
  } else {
    if (IS_MULTI(&Num)) {
      double multi = pow((double)10, (double)Num.multi);
      Int64 tmp = (Int64)multi;
      if ((double)tmp != multi) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        **diagsArea << DgString0("TO_CHAR");  // out of range
        NADELETEBASIC(numstr, (heap));
        NADELETEBASIC(numfmtStr, (heap));
        return -1;
      }

      Int64 value = nValue * tmp;
      if (nValue != (Int64)((Int32)nValue) || tmp != (Int64)((Int32)tmp)) {
        if (tmp != 0 && ((tmp == -1 && nValue < 0 && value < 0) || value / tmp != nValue)) {
          ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
          **diagsArea << DgString0("TO_CHAR");
          NADELETEBASIC(numstr, (heap));
          NADELETEBASIC(numfmtStr, (heap));
          return -1;
        }
      }
      convertInt64ToAscii(abs(value), numstr);
      Num.pre += Num.multi;
    } else {
      if (nValue < 0)
        strncpy(numstr, dataValue + 1, MAXDOUBLEWIDTH);
      else
        strncpy(numstr, dataValue, MAXDOUBLEWIDTH);
    }

    if (nValue < 0) {
      sign = '-';
    } else {
      sign = '+';
    }
    int numstr_pre_len = strlen(numstr);
    if (Num.post) {
      *(numstr + numstr_pre_len) = '.';
      memset(numstr + numstr_pre_len + 1, '0', Num.post);
      *(numstr + numstr_pre_len + Num.post + 1) = '\0';
    }

    /* needs padding? */
    if (numstr_pre_len < Num.pre) out_pre_spaces = Num.pre - numstr_pre_len;
    /* overflowed prefix digit format? */
    else if (numstr_pre_len > Num.pre) {
      Lng32 nFormatCnt = getFomatNodeCount(format) + 1;
      nFormatCnt = min(arg0->getLength(), nFormatCnt);
      fill_str(result, '#', nFormatCnt);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
    if (IS_BLANK(&Num) && strlen(numstr) == 1 && *numstr == '0') {  // e.g. TO_CHAR(0,'B999') ---> '    '(four spaces)
      Lng32 nBlank = getFomatNodeCount(format);
      nBlank = min(arg0->getLength(), nBlank);
      fill_str(result, ' ', nBlank);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
  }

  NUM_processor(format, &Num, result, numstr, out_pre_spaces, sign);
  NADELETEBASIC(numstr, (heap));
  NADELETEBASIC(numfmtStr, (heap));
  return 0;
}

Lng32 ExpNumerFormat::convertInt64ToChar(char *dataValue, char *result, Attributes *arg0, Attributes *arg1,
                                         Attributes *arg2, char *arg1Str, char *arg2Str, CollHeap *heap,
                                         ComDiagsArea **diagsArea) {
  Lng32 arg2len = arg2->getLength();
  char *numfmtStr = new (heap) char[arg2len + 1];
  str_cpy_all(numfmtStr, arg2Str, arg2len);
  numfmtStr[arg2len] = 0;

  int sign = 0;
  FormatNode *format = NULL;
  NUMDesc Num;
  NUMEntry *ent = NUM_fetch(numfmtStr, heap, diagsArea);
  if (!ent) {
    NADELETEBASIC(numfmtStr, (heap));
    return -1;
  }

  format = ent->format;
  assignNumDesc(Num, ent);

  int out_pre_spaces = 0;
  char *numstr = new (heap) char[MAXDOUBLEWIDTH];
  memset(numstr, 0, MAXDOUBLEWIDTH);
  Int64 nValue = lcl_atoi64(dataValue);
  if (IS_ROMAN(&Num)) {
    int_to_roman((Int32)nValue, numstr);
  } else if (IS_EEEE(&Num)) {
    /* we can do it easily because float8 won't lose any precision */
    double val = (double)nValue;
    snprintf(numstr, MAXDOUBLEWIDTH, "%+.*E", Num.post, val);
    /*
     * Swap a leading positive sign for a space.
     */
    if (*numstr == '+') *numstr = ' ';
  } else {
    if (IS_MULTI(&Num)) {
      double multi = pow((double)10, (double)Num.multi);
      Int64 tmp = (Int64)multi;
      if ((double)tmp != multi) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        **diagsArea << DgString0("TO_CHAR");  // out of range
        NADELETEBASIC(numstr, (heap));
        NADELETEBASIC(numfmtStr, (heap));
        return -1;
      }

      Int64 value = nValue * tmp;
      if (nValue != (Int64)((Int32)nValue) || tmp != (Int64)((Int32)tmp)) {
        if (tmp != 0 && ((tmp == -1 && nValue < 0 && value < 0) || value / tmp != nValue)) {
          ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
          **diagsArea << DgString0("TO_CHAR");
          NADELETEBASIC(numstr, (heap));
          NADELETEBASIC(numfmtStr, (heap));
          return -1;
        }
      }
      Num.pre += Num.multi;
      nValue = value;
    }
    convertInt64ToAscii(((nValue >= 0) ? nValue : -nValue), numstr);

    if (nValue < 0) {
      sign = '-';
    } else {
      sign = '+';
    }
    int numstr_pre_len = strlen(numstr);
    if (Num.post) {
      *(numstr + numstr_pre_len) = '.';
      memset(numstr + numstr_pre_len + 1, '0', Num.post);
      *(numstr + numstr_pre_len + Num.post + 1) = '\0';
    }

    /* needs padding? */
    if (numstr_pre_len < Num.pre) out_pre_spaces = Num.pre - numstr_pre_len;
    /* overflowed prefix digit format? */
    else if (numstr_pre_len > Num.pre) {
      Lng32 nFormatCnt = getFomatNodeCount(format) + 1;
      nFormatCnt = min(arg0->getLength(), nFormatCnt);
      fill_str(result, '#', nFormatCnt);
      NADELETEBASIC(numstr, (heap));
      NADELETEBASIC(numfmtStr, (heap));
      return 0;
    }
  }

  NUM_processor(format, &Num, result, numstr, out_pre_spaces, sign);
  NADELETEBASIC(numstr, (heap));
  NADELETEBASIC(numfmtStr, (heap));
  return 0;
}
