
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      10/17/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "common/Platform.h"

#include <stddef.h>
#include <sys/types.h>
#include <regex.h>
#include "exp/exp_clause_derived.h"
#include "exp/exp_like.h"
#include "exp_function.h"
#include "common/unicode_char_set.h"
#include "nchar_mp.h"

///////////////////////////////////////////////////
// class LikePattern
///////////////////////////////////////////////////
LikePattern::LikePattern(const LikePatternString &pattern, CollHeap *exHeap, CharInfo::CharSet cs,
                         CharInfo::Collation co)
    : LikePatternHeader(LikePatternStringIterator::NON_WILDCARD, new (exHeap) char[pattern.getLength()], exHeap),
      error_(EXE_OK) {
  char *patternBuf = getPattern();
  const char *currentChar;

  LikePatternHeader *lastHeader = this;
  LikePatternHeader *prevHeader = NULL;
  LikePatternStringIterator i(pattern);

  unsigned char *headerPattern;
  UInt16 headerLength;
  UInt16 encodedPatternLength = 0;

  NABoolean systemCollationFlag = CollationInfo::isSystemCollation(co);

  UInt16 nPasses;
  int effEncodedKeyLength;

  if (systemCollationFlag)
    nPasses = CollationInfo::getCollationNPasses(co);
  else
    nPasses = 1;

  int number_bytes;
  int len = pattern.getLength();

  while (i != LikePatternStringIterator::END_OF_PATTERN) {
    currentChar = i.getCurrentChar();
    switch (i) {
      case LikePatternStringIterator::NON_WILDCARD:
      case LikePatternStringIterator::UNDERSCORE: {
        //
        // Append the non-wildcard character or underscore to the current header.
        //
        number_bytes = Attributes::getFirstCharLength(currentChar, len, cs);

        if (number_bytes <= 0) {
          error_ = EXE_INVALID_CHARACTER;
          return;
        }

        lastHeader->append(i);

        i += number_bytes + ((cs == CharInfo::UCS2) ? 1 : 0);  // For UCS2, number_bytes is always 1
        i.determineCharType();
        len -= number_bytes;
        break;
      }
      case LikePatternStringIterator::PERCENT: {
        if (i != lastHeader->getLastClause()->getType()) {
          //
          // Append a new header to the LikePattern.
          //
          lastHeader->endClauses();
          patternBuf += lastHeader->getLength();

          if (systemCollationFlag) {
            headerLength = lastHeader->getLength();
            if (headerLength > 0) {
              encodedPatternLength = headerLength * nPasses;
              headerPattern = new (exHeap) unsigned char[encodedPatternLength];

              ex_function_encode::encodeCollationSearchKey((unsigned char *)lastHeader->getPattern(), headerLength,
                                                           headerPattern, encodedPatternLength, effEncodedKeyLength,
                                                           nPasses, co, TRUE);

              lastHeader->setEncodedPattern(headerPattern);
              lastHeader->setEncodedHeader(headerPattern);
            }
            lastHeader->setCollation(co);
          }
          lastHeader->append(new (exHeap) LikePatternHeader(i, patternBuf, exHeap));
          prevHeader = lastHeader;
          lastHeader = prevHeader->getNextHeader();
        }

        i += CharInfo::isSingleByteCharSet(cs) ? 1 : BYTES_PER_NAWCHAR;  // increase 1
        i.determineCharType();
        --len;
        break;
      }
      default:  // LikePatternStringIterator::ERROR:
        error_ = EXE_INVALID_ESCAPE_SEQUENCE;
        return;
    }
  }

  lastHeader->endClauses();
  if (systemCollationFlag) {
    headerLength = lastHeader->getLength();
    if (headerLength > 0) {
      encodedPatternLength = headerLength * nPasses;
      headerPattern = new (exHeap) unsigned char[encodedPatternLength];

      ex_function_encode::encodeCollationSearchKey((unsigned char *)lastHeader->getPattern(), headerLength,
                                                   headerPattern, encodedPatternLength, effEncodedKeyLength, nPasses,
                                                   co, TRUE);
      lastHeader->setEncodedPattern(headerPattern);
      lastHeader->setEncodedHeader(headerPattern);
    }
    lastHeader->setCollation(co);
  }
  //
  // If there are more than two headers in the chain, move the last header to
  // the second position in the chain.  This will make it easier to test for
  // pattern matches by allowing us to check the beginning and end of the
  // pattern first.
  //

  if ((prevHeader != NULL) AND(prevHeader != this)) {
    lastHeader->append(getNextHeader());
    append(lastHeader);
    prevHeader->append(NULL);
  }

  //
  // If the pattern begins with a percent, set the character type
  // appropriately.
  //
  if ((getLength() == 0) AND(getNextHeader() != NULL)) setType(LikePatternStringIterator::PERCENT);
}

LikePattern::~LikePattern() {
  //
  // Delete the pattern buffer.
  //
  NADELETEBASIC(getPattern(), getExHeap());
  //
  // Delete the attached headers.
  //
  LikePatternHeader *header = getNextHeader();
  while (header != NULL) {
    LikePatternHeader *prevHeader = header;
    header = header->getNextHeader();
    delete prevHeader;
  }
}

NABoolean LikePattern::matches(const char *text, UInt16 textLen, CharInfo::CharSet cs) {
  if (text == NULL) return FALSE;

  const char *endOfText = &text[textLen];
  int number_bytes = 0;
  int numChrInHeader = 0;
  int numChrInRecord = 0;
  int numOfChar = 0;
  int charToOffset = 0;
  int headerMatchLen = 0;
  CollHeap *exHeap = getExHeap();

  unsigned char *headerPattern;
  UInt16 headerLength;

  NABoolean equalFlag;
  NABoolean matchFlag;
  NABoolean deallocateNeeded = FALSE;
  UInt16 encodedPatternLength = 0;

  CharInfo::Collation co = getCollation();
  NABoolean systemCollationFlag = CollationInfo::isSystemCollation(co);

  UInt16 nPasses;

  if (systemCollationFlag)
    nPasses = CollationInfo::getCollationNPasses(co);
  else
    nPasses = 1;

  // Check the first partition.
  LikePatternHeader *header = this;
  const char *patternStr = header->getPattern();

  if (header->getType() != LikePatternStringIterator::PERCENT) {
    if (systemCollationFlag) {
      headerPattern = header->getEncodedPattern();
      if ((header->getLength() * nPasses) > (endOfText - text)) return FALSE;
    } else {
      headerPattern = (unsigned char *)header->getPattern();

      numChrInHeader = Attributes::getCharLengthInBuf((const char *)headerPattern,
                                                      (const char *)headerPattern + header->getLength(), NULL, cs);

      numChrInRecord = Attributes::getCharLengthInBuf(text, endOfText, NULL, cs);

      if (numChrInHeader > numChrInRecord) return FALSE;
    }

    matchFlag = header->matches(text, headerMatchLen, cs);

    if (header->error() != EXE_OK) {
      error_ = header->error();
      return FALSE;
    }

    if (NOT matchFlag) return FALSE;

    // If there is only one partition in the pattern, it must match the whole
    // text for there to be a match.
    if (header->getNextHeader() == NULL) return IFX(headerMatchLen == textLen) THENX TRUE ELSEX FALSE;

    text += headerMatchLen;
    patternStr += header->getLength();
  }

  // Check the last partition.
  header = header->getNextHeader();
  headerLength = header->getLength();
  if (headerLength > 0) {
    NABoolean matchLastHeader = header->matchesR(text, endOfText, cs);
    if (header->error() != EXE_OK) {
      error_ = header->error();
      return FALSE;
    }
    if (NOT matchLastHeader) return FALSE;
  }

  // Check the remaining partitions.
  header = header->getNextHeader();
  while (header != NULL)  // check all headers in pattern
  {
    headerLength = header->getLength();
    if (systemCollationFlag) {
      headerPattern = header->getEncodedPattern();
      if (header->getLength() > (endOfText - text)) return FALSE;
    } else {
      headerPattern = (unsigned char *)header->getPattern();

      numChrInHeader = Attributes::getCharLengthInBuf((const char *)headerPattern,
                                                      (const char *)headerPattern + header->getLength(), NULL, cs);

      numChrInRecord = Attributes::getCharLengthInBuf(text, endOfText, NULL, cs);

      if (numChrInHeader > numChrInRecord) return FALSE;
    }

    while (endOfText - text > 0) {
      // check text with the pattern header. If they do not match, move to
      // the next character in text, compare again. Otherwise, work on the
      // next header.

      if (header->matches(text, headerMatchLen, cs)) break;
      if (header->error() != EXE_OK) {
        error_ = header->error();
        return FALSE;
      }

      if (systemCollationFlag) {
        text += nPasses;
      } else {
        // If the text does not match the pattern, move to next character.
        number_bytes = Attributes::getFirstCharLength(text, strlen(text), cs);

        if (number_bytes <= 0) {
          error_ = EXE_INVALID_CHARACTER;
          return FALSE;
        }

        text += number_bytes;
      }

      if (header->getType() == LikePatternStringIterator::NON_WILDCARD) {
        while (text < endOfText) {
          if (systemCollationFlag) {
            equalFlag = TRUE;

            for (short i = 0; i < nPasses; i++) {
              if (*(text + i) != *(header->getEncodedPattern() + i)) {
                equalFlag = FALSE;
                break;
              }
            }
            if (equalFlag)
              break;
            else
              text += nPasses;
          } else {
            number_bytes = Attributes::getFirstCharLength(text, strlen(text), cs);

            if (number_bytes <= 0) {
              error_ = EXE_INVALID_CHARACTER;
              return FALSE;
            }
            equalFlag = TRUE;

            for (short i = 0; i < number_bytes; i++) {
              if (*(text + i) != *(header->getPattern() + i)) {
                equalFlag = FALSE;
                break;
              }
            }
            if (equalFlag)
              break;
            else
              text += number_bytes;
          }
        }
      }
      if (header->getLength() > (endOfText - text)) {
        return FALSE;
      }
    }

    // move the pointers to next pattern header and the remaining text

    patternStr += header->getLength();
    text += headerMatchLen;

    header = header->getNextHeader();
  }
  return TRUE;
}

///////////////////////////////////////////////////
// class ExRegexpClauseBase
///////////////////////////////////////////////////
ex_expr::exp_return_type ExRegexpClauseBase::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  //
  // If an operand is missing(its a null value), set the result to UNKNOWN.
  //
  for (short i = 1; i < getNumOperands(); i++) {
    if (getOperand(i)->getNullFlag() && (NOT op_data[i])) {
      *(int *)op_data[2 * MAX_OPERANDS] = -1;
      return ex_expr::EXPR_NULL;
    }
  }

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////
// class ex_like_clause_base
///////////////////////////////////////////////////
ex_expr::exp_return_type ex_like_clause_base::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  //
  // If an operand is missing(its a null value), set the result to UNKNOWN.
  //
  for (short i = 1; i < getNumOperands(); i++) {
    if (getOperand(i)->getNullFlag() && (NOT op_data[i])) {
      if (i == 3 && (NOT getAllowEscapeNull())) continue;
      *(int *)op_data[2 * MAX_OPERANDS] = -1;
      return ex_expr::EXPR_NULL;
    }
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExRegexpClauseChar::eval(char *op_data[], CollHeap *exHeap, ComDiagsArea **diagsArea) {
  NABoolean matchFlag = true;
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  int len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  char *pattern;
  regmatch_t pm[1];
  const size_t nmatch = 1;
  int cflags, z = 0;
  char *srcStr = new (exHeap) char[len1 + 1];
  char ebuf[128];

  cflags = REG_EXTENDED | REG_NEWLINE;
  pattern = new (exHeap) char[len2 + 1];
  pattern[len2] = 0;
  srcStr[len1] = 0;

  str_cpy_all(pattern, op_data[2], len2);
  str_cpy_all(srcStr, op_data[1], len1);
  if (rpattern_ != pattern) {
    if (rpattern_ != "") regfree(&reg);
    rpattern_ = pattern;
    z = regcomp(&reg, pattern, cflags);
  }

  if (z != 0) {
    // ERROR
    memset(ebuf, 0, sizeof(ebuf));
    regerror(z, &reg, ebuf, sizeof(ebuf));
    ExRaiseSqlError(exHeap, diagsArea, (ExeErrorCode)8453);
    **diagsArea << DgString0(ebuf);
    return ex_expr::EXPR_ERROR;
  }

  z = regexec(&reg, srcStr, nmatch, pm, 0);
  if (z == REG_NOMATCH) {
    matchFlag = false;
  } else if (z != 0) {
    regerror(z, &reg, ebuf, sizeof(ebuf));
    ExRaiseSqlError(exHeap, diagsArea, (ExeErrorCode)8453);
    **diagsArea << DgString0(ebuf);
    return ex_expr::EXPR_ERROR;
  }

  *(int *)op_data[0] = (int)matchFlag;

  NADELETEBASIC(pattern, exHeap);
  NADELETEBASIC(srcStr, exHeap);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_like_clause_char::eval(char *op_data[], CollHeap *exHeap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  if (cs == CharInfo::ISO88591) cs = ((SimpleType *)getOperand(1))->getIsoMapping();

  // get length of operands
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  int len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  int len3 = 0;
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);

    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    len2 = Attributes::trimFillerSpaces(op_data[2], prec2, len2, cs);
  }

  const char *csname = CharInfo::getCharSetName(cs);
  const int smallBufSize = 128;
  unsigned char smallBuf[smallBufSize];

  const char *escapeChar;
  if (getNumOperands() < 4)
    escapeChar = NULL;
  else {
    // get length of escape character
    len3 = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
    if (cs == CharInfo::UTF8) {
      int prec3 = ((SimpleType *)getOperand(3))->getPrecision();
      len3 = Attributes::trimFillerSpaces(op_data[3], prec3, len3, cs);
    }
    escapeChar = op_data[3];

    if (len3 != 1 || (getOperand(3)->getNullFlag() && (NOT op_data[-2 * MAX_OPERANDS + 3]))) {
      ExRaiseSqlError(exHeap, diagsArea, EXE_INVALID_ESCAPE_CHARACTER);
      return ex_expr::EXPR_ERROR;
    }
  }

  CharInfo::Collation co = ((SimpleType *)getOperand(1))->getCollation();

  LikePatternString patternString(op_data[2], (UInt16)len2, cs, escapeChar, len3);
  LikePattern pattern(patternString, exHeap, cs, co);
  if (pattern.error()) {
    if (pattern.error() == EXE_INVALID_ESCAPE_SEQUENCE)
      ExRaiseSqlError(exHeap, diagsArea, EXE_INVALID_ESCAPE_SEQUENCE);
    else {
      ExRaiseSqlError(exHeap, diagsArea, pattern.error());
      *(*diagsArea) << DgString0(csname) << DgString1("LIKE PATTERN");
    }
    return ex_expr::EXPR_ERROR;
  }

  unsigned char *textStr = (unsigned char *)op_data[1];
  unsigned char *encodedText = NULL;
  int encodedPatternLength = 0;
  NABoolean systemCollationFlag = CollationInfo::isSystemCollation(co);
  NABoolean deallocateNeeded = FALSE;

  if (systemCollationFlag) {
    pattern.setCollation(co);
    short nPasses = CollationInfo::getCollationNPasses(co);
    int effEncodedKeyLength;

    encodedPatternLength = len1 * nPasses;

    if (encodedPatternLength <= smallBufSize)
      encodedText = smallBuf;
    else {
      encodedText = new (exHeap) unsigned char[encodedPatternLength];
      deallocateNeeded = TRUE;
    }

    ex_function_encode::encodeCollationSearchKey((unsigned char *)op_data[1], len1, encodedText, nPasses * len1,
                                                 effEncodedKeyLength, nPasses, co, TRUE);

    textStr = encodedText;
    //    len1 = effEncodedKeyLength;  the length without trading space.
    len1 *= nPasses;
  }

  if (cs == CharInfo::UTF8) {
    int Prec = ((SimpleType *)getOperand(1))->getPrecision();
    if (Prec > 0) {
      int endOff = Attributes::convertCharToOffset((const char *)textStr, Prec + 1, len1, cs);
      if (endOff >= 0)  // If no error
        len1 = endOff;
      // else bad UTF8 chars will get detected later by existing code (below).
    }
  }
  NABoolean matchFlag = pattern.matches((char *)textStr, (UInt16)len1, cs);

  if (deallocateNeeded) NADELETEBASIC(encodedText, exHeap);

  if (pattern.error()) {
    ExRaiseSqlError(exHeap, diagsArea, pattern.error());
    *(*diagsArea) << DgString0(csname) << DgString1("LIKE CLAUSE");
    return ex_expr::EXPR_ERROR;
  }

  *(int *)op_data[0] = (int)matchFlag;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_like_clause_doublebyte::eval(char *op_data[], CollHeap *exHeap, ComDiagsArea **diagsArea) {
  // get length of operands
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  int len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);

  NAWchar wPercentChar = 0;
  NAWchar wUnderScoreChar = 0;
  short bpcs;

  switch (getOperand(1)->getCharSet()) {
    case CharInfo::UNICODE:
      wPercentChar = unicode_char_set::percent_char();
      wUnderScoreChar = unicode_char_set::underscore_char();
      bpcs = unicode_char_set::bytesPerChar();
      break;

    case CharInfo::KANJI_MP:
      wPercentChar = kanji_char_set::percent_char();
      wUnderScoreChar = kanji_char_set::underscore_char();
      bpcs = kanji_char_set::bytesPerChar();
      break;

    case CharInfo::KSC5601_MP:
    default:
      wPercentChar = ksc5601_char_set::percent_char();
      wUnderScoreChar = ksc5601_char_set::underscore_char();
      bpcs = ksc5601_char_set::bytesPerChar();
      break;
  }

  const char *escapeChar;
  if (getNumOperands() < 4)
    escapeChar = NULL;
  else {
    // get length of escape character
    int len3 = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
    escapeChar = op_data[3];
    if (len3 != (int)bpcs) {
      ExRaiseSqlError(exHeap, diagsArea, EXE_INVALID_ESCAPE_CHARACTER);
      return ex_expr::EXPR_ERROR;
    }
  }

  LikePatternString patternString(op_data[2], (UInt16)len2, getOperand(1)->getCharSet(), escapeChar, BYTES_PER_NAWCHAR,
                                  (char *)&wUnderScoreChar, BYTES_PER_NAWCHAR, (char *)&wPercentChar,
                                  BYTES_PER_NAWCHAR);
  LikePattern pattern(patternString, exHeap, CharInfo::UNICODE);
  if (pattern.error()) {
    ExRaiseSqlError(exHeap, diagsArea, EXE_INVALID_ESCAPE_SEQUENCE);
    return ex_expr::EXPR_ERROR;
  }

  NABoolean matchFlag = pattern.matches(op_data[1], (UInt16)len1, CharInfo::UCS2);
  if (pattern.error()) {
    ExRaiseSqlError(exHeap, diagsArea, pattern.error());
    const char *csname = CharInfo::getCharSetName(CharInfo::UCS2);
    *(*diagsArea) << DgString0(csname) << DgString1("LIKE CLAUSE");

    return ex_expr::EXPR_ERROR;
  }

  *(int *)op_data[0] = (int)matchFlag;

  return ex_expr::EXPR_OK;
}

NABoolean LikePatternHeader::matches(const char *text, int &headerMatchLen, CharInfo::CharSet cs) {
  headerMatchLen = 0;
  int clauseLen;

  LikePatternClause *clause = this;

  int lenEncoded = getLength();
  char *pattern = getPattern();
  unsigned char *encodedPattern = NULL;

  CharInfo::Collation co = getCollation();
  UInt16 nPasses = 1;
  NABoolean systemCollationFlag = CollationInfo::isSystemCollation(co);

  if (systemCollationFlag) {
    nPasses = CollationInfo::getCollationNPasses(co);
    encodedPattern = getEncodedPattern();
  }

  do {
    if (systemCollationFlag) {
      clause->setEncodedPattern(encodedPattern);
      clause->setCollation(this->getCollation());
    }
    if (NOT clause->matches(text)) return FALSE;
    clauseLen = clause->getLength();
    if (clause->getType() == LikePatternStringIterator::UNDERSCORE) {
      // underscore match character, not byte

      if (systemCollationFlag) {
        int len = clause->getLength() * nPasses;
        text += len;
        encodedPattern += len;
        headerMatchLen += len;
      } else {
        int number_bytes = 1;
        for (int k = 0; k < clause->getLength(); k++) {
          number_bytes = Attributes::getFirstCharLength(text, strlen(text), cs);
          if (number_bytes < 0) {
            setError(EXE_INVALID_CHARACTER);
            return FALSE;
          }
          text += number_bytes;
          headerMatchLen += number_bytes;
        }
      }
    } else {
      if (systemCollationFlag) {
        int len = clauseLen * nPasses;
        text += len;
        encodedPattern += len;
        headerMatchLen += len;
      } else {
        text += clauseLen;
        headerMatchLen += clauseLen;
      }
    }
    clause = clause->getNextClause();
  } while (clause != NULL);

  return TRUE;
}

NABoolean LikePatternHeader::matchesR(const char *text, const char *&endText, CharInfo::CharSet cs) {
  LikePatternClause *clause = this;
  LikePatternClause *clauseR = this;
  const char *p, *p1;
  int firstCharLen;
  CollHeap *heap = getExHeap();
  int bufferLength = endText - text;
  int headerLength = this->getLength();
  char *charLengthInBuf = NULL;
  int numberOfCharInBuf;

  const int smallBufSize = 128;
  char smallBuf[smallBufSize];

  CharInfo::Collation co = getCollation();
  NABoolean systemCollationFlag = CollationInfo::isSystemCollation(co);
  NABoolean deallocateNeeded = FALSE;

  UInt16 nPasses = 1;
  unsigned char *headerPattern;

  if (systemCollationFlag) {
    headerPattern = getEncodedPattern();
    nPasses = CollationInfo::getCollationNPasses(co);
    headerLength *= nPasses;
  } else {
    if (bufferLength <= smallBufSize)
      charLengthInBuf = smallBuf;
    else {
      charLengthInBuf = new (heap) char[bufferLength];
      deallocateNeeded = TRUE;
    }

    headerPattern = (unsigned char *)this->getPattern();
  }

  // Find the last clause in the header
  while (clauseR->getNextClause() != NULL) clauseR = clauseR->getNextClause();

  do {
    // clauseR->getLength() = number of byte of the character in pattern
    if (clauseR->getType() == LikePatternStringIterator::UNDERSCORE) {
      // One underscore match character, not byte. One clause may have more
      // than one underscore.

      int numOfUnderscore = clauseR->getLength();
      if (systemCollationFlag) {
        endText -= (numOfUnderscore * nPasses);
        headerLength -= (clauseR->getLength() * nPasses);
        if (endText < text)  // String left is shorter than the pattern
        {
          if (deallocateNeeded) NADELETEBASIC(charLengthInBuf, heap);
          return FALSE;
        }
      } else {
        numberOfCharInBuf = Attributes::getCharLengthInBuf(text, endText, charLengthInBuf, cs);

        if (numberOfCharInBuf < numOfUnderscore) {
          if (deallocateNeeded) NADELETEBASIC(charLengthInBuf, heap);
          if (numberOfCharInBuf < 0) setError(EXE_INVALID_CHARACTER);
          return FALSE;
        }

        for (int k = 1; k <= numOfUnderscore; k++) endText -= charLengthInBuf[numberOfCharInBuf - k];
      }
    } else {
      p = endText - clauseR->getLength() * nPasses;
      p1 = text;
      // p1 point to the beginning of each character
      if (systemCollationFlag) {
        if (p < p1)  // String left is shorter than the pattern
        {
          if (deallocateNeeded) NADELETEBASIC(charLengthInBuf, heap);
          return FALSE;
        } else {
          clauseR->setCollation(this->getCollation());
          clauseR->setEncodedPattern(getEncodedPattern() + (headerLength - (clauseR->getLength() * nPasses)));
        }
      } else {
        while (p1 < p) {
          firstCharLen = Attributes::getFirstCharLength(p1, endText - p1, cs);
          p1 += firstCharLen;
        }
        if (p1 != p) {
          if (deallocateNeeded) NADELETEBASIC(charLengthInBuf, heap);
          return FALSE;
        }
      }

      if (NOT clauseR->matches(p)) {
        if (deallocateNeeded) NADELETEBASIC(charLengthInBuf, heap);
        return FALSE;
      }
      endText = p;
      headerLength -= (clauseR->getLength() * nPasses);
    }
    clauseR = clauseR->getPreviousClause();
  } while (clauseR);

  if (deallocateNeeded) NADELETEBASIC(charLengthInBuf, heap);

  return TRUE;
}

NABoolean LikePatternClause::matches(const char *text) {
  if (getType() == LikePatternStringIterator::NON_WILDCARD) {
    CharInfo::Collation co = getCollation();

    int clauseLength = getLength();
    unsigned char *pattern;
    UInt16 nPasses;

    if (CollationInfo::isSystemCollation(co)) {
      pattern = encodedPattern_;
      nPasses = CollationInfo::getCollationNPasses(co);
      clauseLength *= nPasses;
    } else
      pattern = (unsigned char *)pattern_;

    for (UInt16 i = 0; i < clauseLength; i++)
      if (pattern[i] != (unsigned char)text[i]) return FALSE;
  }
  return TRUE;
}
