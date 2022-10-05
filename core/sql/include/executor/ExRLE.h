

#ifndef EX_RLE_H
#define EX_RLE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExRLE.cpp
 * Description:  methods to encode/decode using RLE algorithm.
 *
 * Simple RLE encoder/decoder.
 * Provided by Venkat R.
 * This file need to be kept in sync with the corresponding
 * file maintained by NVT.
 *
 * Created:
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"

/**
 * Flag definitions
 */

#define COMP_ALGO_RLE_BASE     0x00000001
#define COMP_ALGO_RLE_PACKBITS 0x00000002

/**
 * Whether single byte, unicode or integer comparison for runs
 **/

#define COMP_COMPARE_BYTE  0x40000000
#define COMP_COMPARE_SHORT 0x20000000

/*
 * Whether checksum is valid
 */
#define COMP_CHECKSUM_VALID 0x80000000

#define EYE_CATCHER_1 0x434F4D50
#define EYE_CATCHER_2 0x524C4500

/*
 * The lengths are always in bytes in the
 * header
 */

typedef struct buf_hdr {
  int compressed_len;
  int eye_catcher1;
  int eye_catcher2;
  int checksum;
  int exploded_len;
  int flags;
} buf_hdr;

#define HDR_SIZE sizeof(buf_hdr)
typedef struct comp_buf {
  union u {
    buf_hdr hdr;
    char byte[HDR_SIZE];
  } u;
  char data[1];
} comp_buf;

/*
 * Simple RLE encoder/decoder
 */

/*
 * Max number of counts we can express per char.  Longer runs will have
 * multiple (char, count) pairs as needed.  Essentiall maximum count we
 * can represent in a byte.
 */
#define MAXCOUNT  0x000000FF
#define MAXUCOUNT 0x0000FFFF
#define EOFCHAR   -1

#define ADLER32_MODULUS 65521

/* minimum run length to encode */
#define RLE_PACKBITS_MINRUN 3

/* Max copy size */
#define RLE_PACKBITS_MAXCOPY_BYTE  128
#define RLE_PACKBITS_MAXCOPY_SHORT 32768

/* maximum run length to encode */
#define RLE_PACKBITS_MAXRUN_BYTE  (RLE_PACKBITS_MAXCOPY_BYTE + RLE_PACKBITS_MINRUN - 1)
#define RLE_PACKBITS_MAXRUN_SHORT (RLE_PACKBITS_MAXCOPY_SHORT + RLE_PACKBITS_MINRUN - 1)

#define RLE_PACKBITS_MAXBUF_BYTE  (RLE_PACKBITS_MAXCOPY_BYTE + RLE_PACKBITS_MINRUN - 1)
#define RLE_PACKBITS_MAXBUF_SHORT (RLE_PACKBITS_MAXCOPY_SHORT + RLE_PACKBITS_MINRUN - 1)

static int encode_byte(unsigned char *dbuf, int dlen, unsigned char *ebuf, int *elen);
static int pb_encode_byte(unsigned char *dbuf, int dlen, unsigned char *ebuf, int *elen);
static int encode_short(unsigned short *dbuf, int dlen_bytes, unsigned short *ebuf, int *elen_bytes);
static int pb_encode_short(unsigned short *dbuf, int dlen_bytes, unsigned short *ebuf, int *elen_bytes);
static int decode_byte(unsigned char *ebuf, int elen, unsigned char *dbuf, int *dlen);
static int pb_decode_byte(unsigned char *ebuf, int elen, unsigned char *dbuf, int *dlen);
static int decode_short(unsigned short *ebuf, int elen_bytes, unsigned short *dbuf, int *dlen_bytes);
static int pb_decode_short(unsigned short *ebuf, int elen_bytes, unsigned short *dbuf, int *dlen_bytes);

int ExEncode(unsigned char *dbuf, int dlen, unsigned char *ebuf, int *elen, int flags);
int ExDecode(unsigned char *ebuf, int elen, unsigned char *dbuf, int *dlen, int &param1, int &param2);

#endif
