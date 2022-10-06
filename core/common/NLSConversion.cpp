

#include "common/NLSConversion.h"
#include "common/charinfo.h"
#include "common/nawstring.h"

NAWString *charToUnicode(int charset, const char *s, int len, CollHeap *h) {
  NAString str(s, len);

  NAWString *ws = NULL;
  if (h)
    ws = new (h) NAWString(charset, str.data(), len, h);
  else
    ws = new NAWString(charset, str.data(), len);

  return ws;
}

NAString *unicodeToChar(const NAWchar *wstr, int wlen, int charset, CollHeap *h, NABoolean allowInvalidChar) {
  int slen;
  switch (charset) {
    case CharInfo::ISO88591:
    case CharInfo::SJIS:
    case CharInfo::EUCJP:
    case CharInfo::GB18030:
    case CharInfo::GB2312:
    case CharInfo::GBK:
    case CharInfo::KSC5601:
    case CharInfo::BIG5:
    case CharInfo::UTF8:
      slen = CharInfo::maxBytesPerChar((CharInfo::CharSet)charset) * wlen;
      break;
    default:
      return NULL;  // We should never get here (unless a new charset is only partially implemented.)
      break;
  }
  char *target = NULL;

  if (!slen) slen = 1;  // Allocate atleast 1 char

  if (h) {
    target = new (h) char[slen];
    if (target == NULL) return 0;
  } else
    target = new char[slen];

  slen = UnicodeStringToLocale(charset, wstr, wlen, target, slen, FALSE, allowInvalidChar);

  NAString *res = NULL;

  if (h) {
    // Only create a return NAString if the conversion was successful.
    if (slen != 0 || wlen == 0) res = new (h) NAString(target, slen, h);
    NADELETEBASIC(target, h);
  } else {
    // Only create a return NAString if the conversion was successful.
    if (slen != 0 || wlen == 0) res = new NAString(target, slen);
    delete[] target;
  }

  return res;
}

NAString *charToChar(int targetCS, const char *s, int sLenInBytes, int sourceCS, NAMemory *h /* = NULL */,
                     NABoolean allowInvalidChar /* = FALSE */) {
  NAString *res = NULL;
  if (s == NULL || sourceCS == (int)CharInfo::UnknownCharSet || targetCS == (int)CharInfo::UnknownCharSet) {
    return NULL;  // error
  }
  if (sLenInBytes == 0) {
    if (h)
      res = new (h) NAString(h);  // empty string
    else
      res = new NAString;
    return res;
  }
  if (targetCS == sourceCS) {
    if (h)
      res = new (h) NAString(s, sLenInBytes, h);  // deep copy
    else
      res = new NAString(s, sLenInBytes);  // deep copy

    return res;
  }

  // targetCS != sourceCS

  if ((CharInfo::CharSet)sourceCS == CharInfo::UCS2) {
    res = unicodeToChar((const NAWchar *)s  // source string
                        ,
                        sLenInBytes / BYTES_PER_NAWCHAR  // src len in NAWchars
                        ,
                        targetCS, h, allowInvalidChar);
    return res;
  }

  // sourceCS != CharInfo::UCS2

  NAWString *wstr = charToUnicode(sourceCS  // src char set
                                  ,
                                  s  // src str
                                  ,
                                  sLenInBytes  // src str len in bytes
                                  ,
                                  h  // heap for allocated target str
  );
  if (wstr == NULL)  // conversion failed
  {
    return NULL;  // error
  }
  if ((CharInfo::CharSet)targetCS == CharInfo::UCS2) {
    if (h)
      res = new (h) NAString((const char *)wstr->data()  // source string
                             ,
                             wstr->length() * BYTES_PER_NAWCHAR  // source len in bytes
                             ,
                             h);
    else
      res = new NAString((const char *)wstr->data()  // source string
                         ,
                         wstr->length() * BYTES_PER_NAWCHAR  // source len in bytes
      );

    delete wstr;
    return res;
  }

  // targetCS != CharInfo::UCS2

  res = unicodeToChar(wstr->data(), wstr->length()  // in NAWchars
                      ,
                      targetCS, h, allowInvalidChar);
  delete wstr;
  return res;
}
