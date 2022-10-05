
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         conversionISO88591.h
 * RCS:          $Id:
 * Description:  The implementation of ISO88591 related conversion routins
 *
 *
 * Created:      7/8/98
 * Modified:     $ $Date: 1998/08/10 16:00:29 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *
 *****************************************************************************
 */

// define MODULE_DEBUG when the module is to be debugged separately.
// #define  MODULE_DEBUG

#include "common/NLSConversion.h"
#include "common/str.h"
#include "common/csconvert.h"
#include "common/charinfo.h"
#include "common/nawstring.h"
#ifdef MODULE_DEBUG
#include "NLSConversion.cpp"
#endif

//
// Conversions between ISO88591 and Unicode.
//
// The Unicode mapping available at http://www.unicode.org/Public/MAPPINGS/
// ISO8859/8859-1.TXT or described in the Unicode Standard Version 3.0 is
// used to construct the two routines.
//
// Note the Microsoft Windows code page 1252 is a super set of ISO88591.
//

NAWcharBuf *ISO88591ToUnicode(const charBuf &input, CollHeap *heap, NAWcharBuf *&unicodeString,
                              NABoolean addNullAtEnd) {
  NAWcharBuf *output = checkSpace(heap, input.getStrLen(), unicodeString, addNullAtEnd);

  if (output == 0) return 0;

  NAWchar *target = output->data();

  int i;
  for (i = 0; i < input.getStrLen(); i++) {
    target[i] = (NAWchar)(input.data()[i]);
  }

  if (addNullAtEnd) target[i] = 0;

  output->setStrLen(input.getStrLen());
  return output;
}

charBuf *unicodeToISO88591(const NAWcharBuf &input, CollHeap *heap, charBuf *&iso88591String, NABoolean addNullAtEnd,
                           NABoolean allowInvalidCodePoint) {
  charBuf *output = checkSpace(heap, input.getStrLen(), iso88591String, addNullAtEnd);

  if (output == 0) return 0;

  unsigned char *target = output->data();

  int i;
  for (i = 0; i < input.getStrLen(); i++) {
    if (input.data()[i] > 0xFF) {
      if (allowInvalidCodePoint)
        target[i] = '?';
      else {
        if (iso88591String == NULL) {
          if (heap)
            NADELETE(output, charBuf, heap);
          else
            delete output;
        }
        return NULL;
      }
    } else
      target[i] = (unsigned char)(input.data()[i]);
  }

  if (addNullAtEnd) target[i] = 0;

  output->setStrLen(input.getStrLen());
  return output;
}

//
// Conversions between cset and Unicode.   (wrapper for new calls)
//
extern cnv_charset convertCharsetEnum(int inset);
NAWcharBuf *csetToUnicode(const charBuf &input, CollHeap *heap, NAWcharBuf *&unicodeString, int cset,
                          int &errorcode, NABoolean addNullAtEnd, int *charCount, int *errorByteOff) {
  char *err_ptr = NULL;
  UInt32 byteCount = 0, lv_charCount = 0, computedMaxBufSizeInNAWchars = 0;
  NABoolean outputBufferAllocatedByThisRoutine = (unicodeString == NULL) ? TRUE : FALSE;
  enum cnv_charset cnvSet = convertCharsetEnum(cset);

  computedMaxBufSizeInNAWchars = (input.getStrLen() + 1) * 2;  // in NAWchar elements for the worst case

  NAWcharBuf *output = checkSpace(heap, computedMaxBufSizeInNAWchars, unicodeString, addNullAtEnd);

  if (output == NULL) {
    errorcode = CNV_ERR_BUFFER_OVERRUN;
    return NULL;
  }

  NAWchar *target = output->data();

  errorcode = LocaleToUTF16(cnv_version1, (const char *)input.data(), input.getStrLen(), (const char *)target,
                            output->getBufSize() * BYTES_PER_NAWCHAR /* in bytes */, cnvSet, err_ptr, &byteCount, 0,
                            addNullAtEnd, &lv_charCount);
  if (errorcode == CNV_ERR_NOINPUT) errorcode = 0;  // empty string is OK
  if (errorByteOff) *errorByteOff = err_ptr - (char *)input.data();
  if (charCount) *charCount = (int)lv_charCount;
  // If errorcode != 0, LocaleToUTF16 will not add the NULL terminator
  if (errorcode == 0 && addNullAtEnd && byteCount > 0) {
    // Exclude the size (in bytes) of the NULL terminator from the byte count.
    if (byteCount > BYTES_PER_NAWCHAR)
      byteCount -= BYTES_PER_NAWCHAR;
    else
      byteCount = 0;
  }

  output->setStrLen /*in_NAWchar_s*/ (byteCount / BYTES_PER_NAWCHAR);  // excluding the NULL terminator

  return output;
}

charBuf *unicodeTocset(const NAWcharBuf &input, CollHeap *heap, charBuf *&csetString, int cset, int &errorcode,
                       NABoolean addNullAtEnd, NABoolean allowInvalidCodePoint, int *charCount, int *errorByteOff) {
  char *err_ptr;
  UInt32 byteCount, lvCharCount;
  enum cnv_charset cnvSet = convertCharsetEnum(cset);
  int cwidth = CharInfo::maxBytesPerChar((CharInfo::CharSet)cset);
  charBuf *output = NULL;
  if (input.data() != NULL && input.getStrLen() > 0) {
    int cSetTargetBufferSizeInBytes = input.getStrLen /*in_NAWchars*/ () * cwidth + 16;  // memory is cheap
    UInt32 cSetTargetStrLenInBytes = 0;
    char *pTempTargetBuf = new (heap) char[cSetTargetBufferSizeInBytes];
    errorcode = UTF16ToLocale(cnv_version1, (const char *)input.data()  // source string
                              ,
                              input.getStrLen() * BYTES_PER_NAWCHAR  // source string length in bytes
                              ,
                              (const char *)pTempTargetBuf  // buffer for the converted string
                              ,
                              cSetTargetBufferSizeInBytes  // target buffer size in bytes
                              ,
                              cnvSet  // convert from UTF16 to cnvSet
                              ,
                              err_ptr, &cSetTargetStrLenInBytes  // out - length in bytes of the converted string
                              ,
                              0, addNullAtEnd, allowInvalidCodePoint);
    NADELETEBASICARRAY(pTempTargetBuf, heap);
    pTempTargetBuf = NULL;
    if (errorcode == 0 && cSetTargetStrLenInBytes > 0)
      output = checkSpace(heap, cSetTargetStrLenInBytes, csetString, addNullAtEnd);
    else  // invoke the old code (i.e., keep the old behavior)
      output = checkSpace(heap, input.getStrLen() * cwidth, csetString, addNullAtEnd);
  } else  // invoke the old code (i.e., keep the old behavior)
    output = checkSpace(heap, input.getStrLen() * cwidth, csetString, addNullAtEnd);

  if (output == 0) {
    errorcode = CNV_ERR_BUFFER_OVERRUN;
    return 0;
  }

  unsigned char *target = output->data();

  errorcode = UTF16ToLocale(cnv_version1, (const char *)input.data(), input.getStrLen() * BYTES_PER_NAWCHAR,
                            (const char *)target, output->getBufSize(), cnvSet, err_ptr, &byteCount, 0, addNullAtEnd,
                            allowInvalidCodePoint, &lvCharCount);
  if (errorcode == CNV_ERR_NOINPUT) errorcode = 0;  // empty string is OK
  if (errorByteOff) *errorByteOff = err_ptr - (char *)input.data();
  if (charCount) *charCount = (int)lvCharCount;

  // If errorcode != 0, LocaleToUTF16 will not add the NULL terminator
  if (errorcode == 0 && addNullAtEnd && byteCount > 0) {
    // Exclude the size (in bytes) of the NULL terminator from the byte count.
    UInt32 nullLen = CharInfo::minBytesPerChar((CharInfo::CharSet)cset);

    if (byteCount > nullLen)
      byteCount -= nullLen;
    else
      byteCount = 0;
  }

  output->setStrLen(byteCount /*in_bytes*/);  // excluding the null terminator from the count
  return output;
}

#ifdef MODULE_DEBUG
int main(int argc, char **argv) {
  charBuf *latin1 = 0;

  NAWchar wbuf[1];
  NAWcharBuf uni(wbuf, 1);

  for (NAWchar i = 0; i < 0xff; i++) {
    wbuf[0] = i;
    latin1 = unicodeToISO88591(uni, 0, latin1);
    if (latin1 && latin1->data()[0] != i) {
      printf("u2l1 test failed\n");
      return 1;
    }
  }

  unsigned char buf[1];
  charBuf ascii(buf, 1);
  NAWcharBuf *unicode = 0;

  for (unsigned char j = 0; j < 0xff; j++) {
    buf[0] = j;
    unicode = ISO88591ToUnicode(ascii, 0, unicode);

    if (unicode && unicode->data()[0] != j) {
      printf("l12u test failed\n");
      return 1;
    }
  }

  wbuf[0] = 0xC0F3;  // negative test
  latin1 = unicodeToISO88591(uni, 0, latin1);
  if (latin1) printf("negative u2l1 test failed\n");

  printf("test pass\n");

  return 0;
}

#endif
