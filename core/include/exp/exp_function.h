
#ifndef EXP_FUNCTION_H
#define EXP_FUNCTION_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:  Expression clauses for functions, like string or datetime
 *               functions
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/SequenceGeneratorAttributes.h"
#include "common/dfs2rec.h"
#include "exp/exp_clause.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_dp2_expr.h"

class ex_function_clause;

// Both op1 and op2 are unsigned long.
// 3 bit left circular shift op1, and xor with op2.
// If (op1 << 3) has sign bit on, clear it and toggle one other bit
// by xor with 0x80000002.
// op2 remains intact.
//
// result and op2 have the identical sign bit value.
#define EXP_SHIFT31_XOR(op1, op2)                                             \
  (((op1)&0x10000000) ? ((((op1) << 3) | ((op1) >> 29)) ^ 0x80000002 ^ (op2)) \
                      : ((((op1) << 3) | ((op1) >> 29)) ^ (op2)))
// Both op1 and op2 are unsigned short
#define EXP_SHIFT15_XOR(op1, op2) \
  (((op1)&0x1000) ? ((((op1) << 3) | ((op1) >> 13)) ^ 0x8002 ^ (op2)) : ((((op1) << 3) | ((op1) >> 13)) ^ (op2)))

#define EXP_SCRAMBLE        0x35
#define MAX_IPV4_STRING_LEN 15
#define MAX_IPV6_STRING_LEN 8 * 4 + 7
#define MIN_IPV4_STRING_LEN 7
#define MIN_IPV6_STRING_LEN 2
#define IPV6_PARTS_NUM      8

UInt32 FastHash(char *data, int length);

class ExFunctionAscii : public ex_function_clause {
 public:
  ExFunctionAscii(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionAscii();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionMaxBytes : public ex_function_clause {
 public:
  ExFunctionMaxBytes(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionMaxBytes();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionChar : public ex_function_clause {
 public:
  ExFunctionChar(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionChar();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionConvertHex : public ex_function_clause {
 public:
  ExFunctionConvertHex(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionConvertHex();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionRepeat : public ex_function_clause {
 public:
  ExFunctionRepeat(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionRepeat();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionReplace : public ex_function_clause {
 public:
  ExFunctionReplace(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionReplace();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);
  // ---------------------------------------------------------------------
  // collation
  // ---------------------------------------------------------------------

  CharInfo::Collation getCollation() { return (CharInfo::Collation)collation_; }

  void setCollation(CharInfo::Collation c) { collation_ = (Int16)c; }

  Int16 getArgEncodedLen(Int16 i) {
    assert(i < 2 && i >= 0);
    return argsEncodedLen_[i];
  }

  void setArgEncodedLen(Int16 v, Int16 i) {
    assert(i < 2 && i >= 0);
    argsEncodedLen_[i] = v;
  }

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  Int16 collation_;
  Int16 argsEncodedLen_[2];
  char fillers_[2];

  // ---------------------------------------------------------------------
};

class ExRegexpReplace : public ex_function_clause {
 public:
  // Construction
  //
  ExRegexpReplace(){};
  ExRegexpReplace(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space);

  // Execution
  //
  ex_expr::exp_return_type eval(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);

  // Fixup
  //
  Long pack(void *);

  // Null Semantics
  int isNullInNullOut() const { return 0; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 00-07
};

// Since REGEXP_SUBSTR and REGEXP_COUNT are quite similar,
// we use ExRegexpSubstrOrCount to implement these two functions.
class ExRegexpSubstrOrCount : public ex_function_clause {
 public:
  // Construction
  //
  ExRegexpSubstrOrCount(){};
  ExRegexpSubstrOrCount(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space,
                        NABoolean bIsSubstr);

  // Execution
  //
  ex_expr::exp_return_type eval(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);

  // Fixup
  //
  Long pack(void *);

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  NABoolean bIsSubstr_;  // 00-03
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[4];  // 04-07
};

class ex_function_char_length : public ex_function_clause {
 public:
  ex_function_char_length(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_char_length();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_oct_length : public ex_function_clause {
 public:
  ex_function_oct_length(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_oct_length();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_char_length_doublebyte : public ex_function_clause {
 public:
  ex_function_char_length_doublebyte(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_char_length_doublebyte();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_position : public ex_function_clause {
 public:
  ex_function_position(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int in_args_num);
  ex_function_position();

  ////////////////////////////////////////////////////////
  // Routine to find position of pattern string within
  // source string.  For use with ISO88591 chars only.
  // Caller is to verify that lengths are > 0.  This
  // function does check, however, to see if the pattern
  // length is <= the source length.
  //
  // Furthermore, this function takes a boolean flag
  // called "patternInFront" which indicates that the
  // caller only wants to check if the pattern resides at
  // the beginning of the string (and not waste time
  // checking the entire string).  This is used by LIKE
  // clauses.
  ////////////////////////////////////////////////////////
  static int findPosition(char *pat, int patLen, char *src, int srcLen, NABoolean patternInFront);

  ////////////////////////////////////////////////////////
  // If searchLen is <= 0 or searchLen > sourceLen or
  // searchStr is not present in sourceStr,
  // return a position of 0;
  // otherwise return the position of searchStr in
  // sourceStr.
  ////////////////////////////////////////////////////////
  static int findPosition(char *sourceStr, int sourceLen, char *searchStr, int searchLen, short bytesPerChar = 1,
                          Int16 nPasses = 1, CharInfo::Collation collation = CharInfo::DefaultCollation,

                          // returns number of characters, if passed in
                          int *numChars = NULL,

                          CharInfo::CharSet cs = CharInfo::ISO88591);

  static int findPositionInReverse(char *sourceStr, int sourceLen, char *searchStr, int searchLen, int occurrence,
                                   CharInfo::CharSet cs = CharInfo::ISO88591);

  static int errorChecks(int startPos, int occurrence,
                         NABoolean isInstr,  // INSTR function
                         const char *userTextStr, CharInfo::Collation collation, CharInfo::CharSet cs, CollHeap *heap,
                         ComDiagsArea **diagsArea);

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  int isNullRelevant() const { return getIsNullRelevant(); }

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *heap, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  // ---------------------------------------------------------------------
  // collation.
  // ---------------------------------------------------------------------

  CharInfo::Collation getCollation() { return (CharInfo::Collation)collation_; }

  void setCollation(CharInfo::Collation v) { collation_ = (Int16)v; }

  const NABoolean getIsInstr() const { return ((flags_ & IS_INSTR) != 0); };

  void setIsInstr(NABoolean v) { (v) ? flags_ |= IS_INSTR : flags_ &= ~IS_INSTR; };

  const NABoolean getIsNullRelevant() const { return ((flags_ & IS_NULL_RELEVANT) != 0); };

  void setIsNullRelevant(NABoolean v) { (v) ? flags_ |= IS_NULL_RELEVANT : flags_ &= ~IS_NULL_RELEVANT; };

 private:
  enum {
    // was this INSTR function
    IS_INSTR = 0x0001,

    // is null processing to be done outside of eval method
    IS_NULL_RELEVANT = 0x0002,
  };

  Int16 collation_;

  Int16 flags_;

  char fillers_[4];
};

class ex_function_concat : public ex_function_clause {
 public:
  ex_function_concat(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_concat();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_lower : public ex_function_clause {
 public:
  ex_function_lower(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_lower();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_lower_unicode : public ex_function_clause {
 public:
  ex_function_lower_unicode(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_lower_unicode();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_upper : public ex_function_clause {
 public:
  ex_function_upper(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_upper();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_upper_unicode : public ex_function_clause {
 public:
  ex_function_upper_unicode(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_upper_unicode();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_substring : public ex_function_clause {
 public:
  ex_function_substring(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space);
  ex_function_substring();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_trim : public ex_function_clause {
 public:
  ex_function_trim(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode);
  ex_function_trim(){};

  inline int getTrimMode() const { return mode_; }
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0) = 0;
  Long pack(void *) = 0;

  // ---------------------------------------------------------------------
  // collation
  // ---------------------------------------------------------------------

  CharInfo::Collation getCollation() { return (CharInfo::Collation)collation_; }

  void setCollation(CharInfo::Collation c) { collation_ = (Int16)c; }

  Int16 getSrcStrEncodedLength() { return srcStrEncodedLength_; }

  void setSrcStrEncodedLength(Int16 v) { srcStrEncodedLength_ = v; }

  Int16 getTrimCharEncodedLength() { return trimCharEncodedLength_; }

  void setTrimCharEncodedLength(Int16 v) { trimCharEncodedLength_ = v; }
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int mode_;                     // 00-03
  Int16 collation_;              // 04-05
  Int16 srcStrEncodedLength_;    // 06-07
  Int16 trimCharEncodedLength_;  // 08-09
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[6];  // 10-15
};

class ex_function_trim_char : public ex_function_trim {
 public:
  ex_function_trim_char(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode);
  ex_function_trim_char();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(3, getClassVersionID());
    ex_function_trim::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionCrc32 : public ex_function_clause {
 public:
  ExFunctionCrc32(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionCrc32();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionSha : public ex_function_clause {
 public:
  ExFunctionSha(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionSha();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionSha2 : public ex_function_clause {
 public:
  ExFunctionSha2(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode);
  ExFunctionSha2();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int mode;
};

class ExFunctionSoundex : public ex_function_clause {
 public:
  ExFunctionSoundex(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionSoundex();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionMd5 : public ex_function_clause {
 public:
  ExFunctionMd5(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionMd5();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  int isNullInNullOut() const { return 0; };
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionIsIP : public ex_function_clause {
 public:
  ExFunctionIsIP(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionIsIP();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionInetAton : public ex_function_clause {
 public:
  ExFunctionInetAton(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionInetAton();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionInetNtoa : public ex_function_clause {
 public:
  ExFunctionInetNtoa(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionInetNtoa();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionAESEncrypt : public ex_function_clause {
 public:
  ExFunctionAESEncrypt(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int args_num, int aes_mode);
  ExFunctionAESEncrypt();

  ex_expr::exp_return_type eval(char *op_datap[], CollHeap *, ComDiagsArea ** = 0);
  long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  // ---------------------------------------------------------------------
 private:
  int aes_mode;
  int args_num;
};

class ExFunctionAESDecrypt : public ex_function_clause {
 public:
  ExFunctionAESDecrypt(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int args_num, int aes_mode);

  ExFunctionAESDecrypt();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  // ---------------------------------------------------------------------
 private:
  int aes_mode;
  int args_num;
};

class ExFunctionBase64EncDec : public ex_function_clause {
 public:
  ExFunctionBase64EncDec(OperatorTypeEnum oper_type, Attributes **attr, Space *space, NABoolean isEncode,
                         int maxBuflen);
  ExFunctionBase64EncDec();

  ex_expr::exp_return_type eval(char *op_datap[], CollHeap *, ComDiagsArea ** = 0);
  //  long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  // ---------------------------------------------------------------------
 private:
  short decodeValue(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea);

  NABoolean isEncode_;  // TRUE, encode. FALSE, decode
  int maxBuflen_;       // max buffer size for value to be decoded.

  // this field is allocated at runtime
  char *decodedBuf_;
};

class ExFunctionTokenStr : public ex_function_clause {
 public:
  ExFunctionTokenStr(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space);
  ExFunctionTokenStr();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionReverseStr : public ex_function_clause {
 public:
  ExFunctionReverseStr(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionReverseStr();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  //  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_trim_doublebyte : public ex_function_trim {
 public:
  ex_function_trim_doublebyte(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode);
  ex_function_trim_doublebyte();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(3, getClassVersionID());
    ex_function_trim::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_sleep : public ex_function_clause {
 public:
  ex_function_sleep(OperatorTypeEnum oper_type, short numOperands, Attributes **attr, Space *space);
  ex_function_sleep();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_unixtime : public ex_function_clause {
 public:
  ex_function_unixtime(OperatorTypeEnum oper_type, short num, Attributes **attr, Space *space);
  ex_function_unixtime();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_current : public ex_function_clause {
 public:
  ex_function_current(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_current();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

// ++ Triggers -
class ex_function_unique_execute_id : public ex_function_clause {
 public:
  ex_function_unique_execute_id(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_unique_execute_id();
  // this eval is dummy, the value is set in ex_root
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0) { return ex_expr::EXPR_OK; };
  Long pack(void *);
};

// ++ Triggers -
class ex_function_get_triggers_status : public ex_function_clause {
 public:
  ex_function_get_triggers_status(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_get_triggers_status();
  // this eval is dummy, the value is set in ex_root
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0) { return ex_expr::EXPR_OK; };
  Long pack(void *);
};

// ++ Triggers -
class ex_function_get_bit_value_at : public ex_function_clause {
 public:
  ex_function_get_bit_value_at(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_get_bit_value_at();
  // this eval is dummy, the value is set in ex_root
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
};

// ++ MV
class ex_function_is_bitwise_and_true : public ex_function_clause {
 public:
  ex_function_is_bitwise_and_true(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_is_bitwise_and_true();
  // this eval is dummy, the value is set in ex_root
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  virtual short getClassSize() { return (short)sizeof(*this); }
};

class ex_function_substring_doublebyte : public ex_function_clause {
 public:
  ex_function_substring_doublebyte(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space);
  ex_function_substring_doublebyte();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_translate : public ex_function_clause {
 public:
  ex_function_translate(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int conv_type, Int16 flags);
  ex_function_translate(){};

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  int get_conv_type() { return conv_type_; };

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  // flags:
  // 0x0001 set the CONV_ALLOW_INVALID_CODE_VALUE flag when converting
  //        the data to allow invalid code points and replace them
  //        with a replacement character
  enum TranslateFlags { TRANSLATE_FLAG_ALLOW_INVALID_CODEPOINT = 0x001 };

  // ---------------------------------------------------------------------

 private:
  int conv_type_;  // 00-03
  // flags, see TranslateFlags enum above
  Int16 flags_;  // 04-05
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[2];  // 06-07
};

class ex_function_encode : public ex_function_clause {
 public:
  // Construction
  //
  ex_function_encode();
  ex_function_encode(OperatorTypeEnum oper_type, Attributes **attr, Space *space, short descFlag = 0);

  ex_function_encode(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                     CharInfo::Collation collation = CharInfo::DefaultCollation, short descFlag = 0,
                     CollationInfo::CollationType collType = CollationInfo::Sort);

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };
  int isNullRelevant() const { return 1; };
  ex_expr::exp_return_type processNulls(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  // Execution
  //
  int isEvalRelevant() const { return 1; };
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  // Fixup
  //
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // Helpers
  //
  static void encodeKeyValue(Attributes *attr, const char *source, const char *varlen_ptr, char *target,
                             NABoolean isCaseInsensitive, Attributes *tgtAttr = NULL, char *tgt_varlen_ptr = NULL,
                             const int tgtLength = 0, CharInfo::Collation collation = CharInfo::DefaultCollation,
                             CollationInfo::CollationType collType = CollationInfo::Sort);

  static void encodeCollationKey(const UInt8 *src, int srcLength, UInt8 *encodedKey, const int encodedKeyLength,
                                 Int16 nPasses, CharInfo::Collation collation = CharInfo::DefaultCollation,
                                 NABoolean rmTSpaces = TRUE);

  static void encodeCollationSearchKey(const UInt8 *src, int srcLength, UInt8 *encodedKey, const int encodedKeyLength,
                                       int &effEncodedKeyLength, Int16 nPasses,
                                       CharInfo::Collation collation = CharInfo::DefaultCollation,
                                       NABoolean rmTSpaces = TRUE);

  ex_expr::exp_return_type evalDecode(char *op_data[], CollHeap *heap);

  static short decodeKeyValue(Attributes *attr, NABoolean isDesc, char *source, char *varlen_ptr, char *target,
                              char *target_varlen_ptr, NABoolean handleNullability);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  NABoolean isDesc() { return ((flags_ & IS_DESC) != 0); };

  inline void setIsDesc(NABoolean v) { (v) ? flags_ |= IS_DESC : flags_ &= ~IS_DESC; };

  NABoolean caseInsensitive() { return ((flags_ & CASE_INSENSITIVE) != 0); };

  inline void setCaseInsensitive(NABoolean v) { (v) ? flags_ |= CASE_INSENSITIVE : flags_ &= ~CASE_INSENSITIVE; };

  NABoolean regularNullability() { return ((flags_ & REGULAR_NULLABILITY) != 0); };

  inline void setRegularNullability(NABoolean v) {
    (v) ? flags_ |= REGULAR_NULLABILITY : flags_ &= ~REGULAR_NULLABILITY;
  };

  NABoolean isDecode() { return ((flags_ & IS_DECODE) != 0); };

  inline void setIsDecode(NABoolean v) { (v) ? flags_ |= IS_DECODE : flags_ &= ~IS_DECODE; };

  static unsigned char getCollationWeight(CharInfo::Collation collation, Int16 pass, UInt16 chr);

  static void getCollationWeight(CharInfo::Collation collation, Int16 pass, UInt16 chr, UInt8 *weightStr,
                                 Int16 &weightStrLen);

  CollationInfo::CollationType getCollEncodingType() const { return (CollationInfo::CollationType)CollEncodingType_; };

  void setCollEncodingType(CollationInfo::CollationType v) { CollEncodingType_ = (UInt8)v; };

  CharInfo::Collation getCollation() const { return (CharInfo::Collation)collation_; }

  void setCollation(CharInfo::Collation v) {
    collation_ = (Int16)v;
    ;
  }

  static Int16 getNumberOfDigraphs(CharInfo::Collation collation);

  static UInt8 *getDigraph(const CharInfo::Collation collation, const int digraphNum);

  static Int16 getDigraphIndex(const CharInfo::Collation collation, const int digraphNum);

  static NABoolean getRmTSpaces(const CharInfo::Collation collation);

  static NABoolean getNumberOfChars(const CharInfo::Collation collation);

  static NABoolean isOneToOneCollation(const CharInfo::Collation collation);

 private:
  enum {
    // encode for case insensitive comparison for character datatypes.
    IS_DESC = 0x0001,
    CASE_INSENSITIVE = 0x0002,
    REGULAR_NULLABILITY = 0x0004,
    IS_DECODE = 0x0008
  };

  Int16 flags_;             // 00-01
  Int16 collation_;         // 02-03
  UInt8 CollEncodingType_;  // 04-04
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[3];  // 05-07
};

// ---------------------------------------------------------------------
// METHOD:  Collated_cmp()
// PURPOSE: Compare 2 strings using specified collation
// RETCODE:  0 - The two strings collate as EQUAL to each other
//          < 0 if string1 < string2
//          > 0 if string1 > string2
// INPUT:   char *string1: Ptr to first string
//          char *string2: Ptr to second string
//          int length:    Amt to compare (in bytes)
//          CharInfo::Collation Collation: Enum value for Collation
//          char *sortBuffer1 - buffer to hold encoded Collation Key 1
//          char *sortBuffer2 - buffer to hold encoded Collation Key 2
// NOTE:    sortBuffer1 & sortBuffer2 MUST BE preallocated and big enough.
// ---------------------------------------------------------------------
// NOTE: No need to use ... maybe in the future?
static int Collated_cmp(char *string1, char *string2, int length, CharInfo::Collation Collation, char *sortBuffer1,
                        char *sortBuffer2) {
  Int16 nPasses = CollationInfo::getCollationNPasses(Collation);
  int encodeKeyBufLen = length * nPasses + 2 + nPasses;

  ex_function_encode::encodeCollationKey((const UInt8 *)string1, length, (UInt8 *)sortBuffer1, encodeKeyBufLen, nPasses,
                                         Collation, FALSE /*remove trailing spaces */
  );
  ex_function_encode::encodeCollationKey((const UInt8 *)string2, length, (UInt8 *)sortBuffer2, encodeKeyBufLen, nPasses,
                                         Collation, FALSE /*remove trailing spaces */
  );
  return (str_cmp(sortBuffer1, sortBuffer2, encodeKeyBufLen));
};

//-- Czech collation info START
struct collationParams {
  Int16 numberOfPasses;
  Int16 numberOfChars;
  Int16 numberOfDigraphs;
  Int16 numberOfLigatures;
  NABoolean rmTSpaces;  // for varchar. for char we trim Tspaces
  UInt8 digraphs[10][2];
  Int16 digraphIdx[10];
  UInt8 weightTable[4][300];  // need to change 4 to collationInfo::maxNbrePAsses after defining it in charinfo.h
};

typedef struct collationParams CollationParams;

static const CollationParams collParams[3] = {
    {    // XCZECH
     2,  // collationNPasses[0],
     259,
     3,
     0,
     0,
     {{'c', 'h'}, {'C', 'h'}, {'C', 'H'}},
     {256, 257, 258},
     // XCZECH weights
     {{0,   0,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 0,   0,   0,   0,   0,   0,   0,   20,
       25,  30,  35,  40,  45,  50,  55,  60,  65,  70,  75,  80,  85,  90,  95,  98,  100, 105, 110, 115, 120, 122,
       125, 130, 135, 0,   0,   0,   0,   0,   0,   20,  25,  30,  35,  40,  45,  50,  55,  60,  65,  70,  75,  80,
       85,  90,  95,  98,  100, 105, 110, 115, 120, 122, 125, 130, 135, 0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   20,  0,   75,  0,   75,  105, 0,   0,   107, 105, 110, 135, 0,   138, 135,
       0,   20,  0,   75,  0,   75,  105, 0,   0,   107, 105, 110, 135, 0,   138, 135, 100, 20,  20,  20,  20,  75,
       30,  30,  32,  40,  40,  40,  40,  60,  60,  35,  35,  85,  85,  90,  90,  90,  90,  0,   102, 115, 115, 115,
       115, 130, 110, 105, 100, 20,  20,  20,  20,  75,  30,  30,  32,  40,  40,  40,  40,  60,  60,  35,  35,  85,
       85,  90,  90,  90,  90,  0,   102, 115, 115, 115, 115, 130, 110, 0,   57,  57,  57},
      {0,   1,   2,   3,   4,   5,   6,   7,   8,   2,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
       22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  1,   33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
       44,  45,  46,  47,  1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   58,  59,  60,  61,  62,  63,  64,  2,
       2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   2,   3,   2,   2,   2,   2,
       2,   2,   2,   91,  92,  93,  94,  95,  96,  1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
       1,   1,   1,   1,   1,   2,   1,   1,   1,   1,   1,   1,   1,   123, 124, 125, 126, 127, 128, 129, 130, 131,
       132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
       154, 155, 156, 157, 158, 159, 160, 28,  216, 32,  164, 14,  5,   167, 168, 14,  26,  14,  4,   173, 14,  22,
       176, 27,  219, 31,  180, 13,  4,   199, 184, 13,  25,  13,  3,   221, 13,  21,  4,   4,   12,  16,  10,  4,
       22,  26,  14,  4,   28,  22,  14,  4,   8,   14,  32,  22,  14,  4,   8,   18,  10,  215, 14,  6,   4,   18,
       10,  4,   26,  1,   3,   3,   11,  15,  9,   3,   21,  25,  13,  3,   27,  21,  13,  3,   7,   13,  31,  21,
       13,  3,   7,   17,  9,   247, 13,  5,   3,   17,  9,   3,   25,  217, 1,   2,   3}}

    },
    {    // XCZECH_CI
     2,  // collationNPasses[1],
     259,
     3,
     0,
     0,
     {{'c', 'h'}, {'C', 'h'}, {'C', 'H'}},
     {256, 257, 258},
     {{0,   0,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 0,   0,   0,   0,   0,   0,   0,   20,
       25,  30,  35,  40,  45,  50,  55,  60,  65,  70,  75,  80,  85,  90,  95,  98,  100, 105, 110, 115, 120, 122,
       125, 130, 135, 0,   0,   0,   0,   0,   0,   20,  25,  30,  35,  40,  45,  50,  55,  60,  65,  70,  75,  80,
       85,  90,  95,  98,  100, 105, 110, 115, 120, 122, 125, 130, 135, 0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   20,  0,   75,  0,   75,  105, 0,   0,   107, 105, 110, 135, 0,   138, 135,
       0,   20,  0,   75,  0,   75,  105, 0,   0,   107, 105, 110, 135, 0,   138, 135, 100, 20,  20,  20,  20,  75,
       30,  30,  32,  40,  40,  40,  40,  60,  60,  35,  35,  85,  85,  90,  90,  90,  90,  0,   102, 115, 115, 115,
       115, 130, 110, 105, 100, 20,  20,  20,  20,  75,  30,  30,  32,  40,  40,  40,  40,  60,  60,  35,  35,  85,
       85,  90,  90,  90,  90,  0,   102, 115, 115, 115, 115, 130, 110, 0,   57,  57,  57},
      {0,   1,   2,   3,   4,   5,   6,   7,   8,   2,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
       22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  1,   33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
       44,  45,  46,  47,  1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   58,  59,  60,  61,  62,  63,  64,  1,
       1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
       1,   1,   1,   91,  92,  93,  94,  95,  96,  1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,
       1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   1,   123, 124, 125, 126, 127, 128, 129, 130, 131,
       132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
       154, 155, 156, 157, 158, 159, 160, 27,  216, 31,  164, 13,  3,   167, 168, 13,  25,  13,  3,   173, 13,  21,
       176, 27,  219, 31,  180, 13,  3,   199, 184, 13,  25,  13,  3,   221, 13,  21,  3,   3,   11,  15,  9,   3,
       21,  25,  13,  3,   27,  21,  13,  3,   7,   13,  31,  21,  13,  3,   7,   17,  9,   215, 13,  5,   3,   17,
       9,   3,   25,  0,   3,   3,   11,  15,  9,   3,   21,  25,  13,  3,   27,  21,  13,  3,   7,   13,  31,  21,
       13,  3,   7,   17,  9,   247, 13,  5,   3,   17,  9,   3,   25,  217, 1,   1,   1}}

    }

    ,

    {// Czech standard

     4,  // collationNPasses[2],
     259,
     3,
     0,
     1,
     {{'c', 'h'}, {'C', 'h'}, {'C', 'H'}},
     {256, 257, 258},
     {{0,  0,  0,  0,  0,  0,  0,  0,  0,  4,  4,  4,  4,  4,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
       0,  0,  0,  0,  0,  0,  4,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  36, 37, 38, 39,
       40, 41, 42, 43, 44, 45, 0,  0,  0,  0,  0,  0,  0,  5,  6,  7,  9,  10, 11, 12, 13, 15, 16, 17, 18, 19,
       20, 21, 22, 23, 24, 26, 28, 29, 30, 31, 32, 33, 34, 0,  0,  0,  0,  0,  0,  5,  6,  7,  9,  10, 11, 12,
       13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 28, 29, 30, 31, 32, 33, 34, 0,  0,  0,  0,  0,  48, 53,
       53, 55, 53, 53, 55, 55, 53, 55, 53, 55, 55, 55, 55, 55, 46, 54, 54, 56, 54, 56, 56, 56, 54, 56, 56, 56,
       56, 56, 56, 56, 4,  5,  0,  18, 0,  18, 26, 0,  0,  27, 26, 28, 34, 0,  35, 34, 0,  5,  0,  18, 0,  18,
       26, 0,  0,  27, 26, 28, 34, 0,  35, 34, 24, 5,  5,  5,  5,  18, 7,  7,  8,  10, 10, 10, 10, 15, 15, 9,
       9,  20, 20, 21, 21, 21, 21, 0,  25, 29, 29, 29, 29, 33, 28, 26, 24, 5,  5,  5,  5,  18, 7,  7,  8,  10,
       10, 10, 10, 15, 15, 9,  9,  20, 20, 21, 21, 21, 21, 0,  25, 29, 29, 29, 29, 33, 28, 0,  14, 14, 14},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
       1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
       1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
       1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 6, 0, 4, 0, 3, 3, 0, 0, 1, 4, 2, 2, 0, 1, 3, 0, 6, 0, 4, 0, 3, 3, 0, 0,
       1, 4, 2, 2, 0, 1, 3, 2, 2, 3, 4, 5, 2, 2, 3, 1, 2, 5, 4, 3, 2, 3, 3, 2, 3, 2, 2, 3, 5, 4, 0, 1, 3, 2, 5, 4, 2,
       3, 1, 2, 2, 3, 4, 5, 2, 2, 3, 1, 2, 5, 4, 3, 2, 3, 3, 2, 3, 2, 2, 3, 5, 4, 0, 1, 3, 2, 5, 4, 2, 3, 0, 1, 1, 1},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 3, 1, 4, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0,  0,  0, 0, 0, 5,
       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1, 0, 0, 0,  0,  0, 0, 0, 2,
       2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,  2, 0, 0, 0,  0,  0, 0, 1, 1,
       1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  0, 0, 0, 0,  27, 6, 4, 5, 3,
       6, 2, 1, 5, 3, 4, 1, 8, 2, 6, 7, 9, 8, 2, 3, 5, 4, 2, 3, 7, 1, 6, 1, 10, 4, 8, 9, 11, 6,  2, 0, 2, 0,
       2, 2, 0, 0, 2, 2, 2, 2, 0, 2, 2, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 1,  0, 1, 1, 2,  2,  2, 2, 2, 2,
       2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 2, 2, 2, 2, 2, 2,  2, 1, 1, 1,  1,  1, 1, 1, 1,
       1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1,  0, 1, 2, 3},
      {0,   0,   0,   0,   0,   0,   0,   0,   0,   255, 255, 255, 255, 255, 0,   0,   0,   0,   0,   0,   0,   0,
       0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   255, 10,  14,  64,  52,  48,  44,  74,  28,  30,  66,  62,
       4,   18,  2,   24,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 12,  6,   36,  60,  38,  8,   58,  255,
       255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
       255, 255, 255, 32,  26,  34,  84,  56,  76,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
       255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 40,  22,  42,  78,  255, 54,  255, 255, 255,
       255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 16,  255, 255, 255, 255, 255, 255, 255, 255, 255,
       255, 255, 255, 255, 255, 255, 255, 255, 86,  255, 50,  255, 255, 46,  90,  255, 255, 255, 255, 20,  255, 255,
       72,  255, 94,  255, 82,  255, 255, 88,  96,  255, 255, 255, 255, 92,  255, 255, 255, 255, 255, 255, 255, 255,
       255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 68,  255, 255, 255, 255,
       255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
       255, 255, 255, 255, 255, 70,  255, 255, 255, 255, 255, 255, 255, 80,  255, 255, 255}

     }

    }

};
//-- Czech collation info END
class ex_function_explode_varchar : public ex_function_clause {
 public:
  ex_function_explode_varchar(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space,
                              NABoolean forInsert);

  ex_function_explode_varchar();

  // Null Semantics
  //
  int isNullInNullOut() const { return 0; };
  int isNullRelevant() const { return 1; };

  // Execution
  //
  int isEvalRelevant() const { return 1; };

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual ex_expr::exp_return_type processNulls(char *op_data[], CollHeap * = 0, ComDiagsArea ** = 0);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int forInsert_;  // 00-03
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[4];  // 04-07
};

class ex_function_hash : public ex_function_clause {
 public:
  ex_function_hash(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_hash();

  int isNullInNullOut() const { return 0; };
  int isNullRelevant() const { return 0; };
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 00-07
  int HashHash(int inValue);
};

class ex_function_hivehash : public ex_function_clause {
 public:
  ex_function_hivehash(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_hivehash();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f) { return ex_clause::pCodeGenerate(space, f); };

  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

 protected:
  virtual int hashForCharType(char *, int);
  virtual int hashForDateType(char *, int);
  virtual int hashForTimestampType(char *, int);
  virtual int hashForIntType(int *);
  virtual int hashForLargeIntType(long *);
  virtual int hashForFloatType(float *);
  virtual int hashForDoubleType(double *);

 private:
  char fillers_[8];  // 00-07
  int getLocalTimeZone();
  int getDateSeconds(char *data, bool calcSecond);
  int getMicroseconds(char *data, int length);
};

class ExHashComb : public ex_function_clause {
 public:
  ExHashComb(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExHashComb();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExHiveHashComb : public ex_function_clause {
 public:
  ExHiveHashComb(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExHiveHashComb();

  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f) { return ex_clause::pCodeGenerate(space, f); };

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

// 256 pregenerated randome 32-bit values.
// Do not edit.
static const unsigned int randomHashValues[256] = {
    0x905ebe29, 0x95ff0b84, 0xe5357ed6, 0x2cffae90, 0x8350b3f1, 0x1748a7eb, 0x2a0695db, 0x1e7ca00c, 0x60f80c24,
    0x9a41fe1c, 0xa985a647, 0x0ed7e512, 0xcd34ef43, 0xe06325a6, 0xecbf735a, 0x76540d38, 0x35cba55d, 0xff539efc,
    0x64545d45, 0xd7112c0d, 0x17e09e1c, 0x02359d32, 0x45976350, 0xd630a578, 0x34cd0c12, 0x754546f6, 0x1bf4f249,
    0xbc65c34f, 0x5c932f44, 0x6cb0d8d0, 0xfd0e7030, 0x2b160e3b, 0x101daff6, 0x25bbcb9d, 0xe7eca21f, 0x6d3b24ca,
    0xaef7e6b9, 0xd212f049, 0x2de2817e, 0x2792bcd5, 0x67f794b2, 0xaec6f7cc, 0x79a3e367, 0xd5a85114, 0xa98ecc2d,
    0xf373e266, 0x58ae2757, 0xd8faa0ff, 0x45e7eb61, 0xbd72ba1e, 0xc28f6b16, 0x804bc2e6, 0xfed74984, 0x881cd177,
    0xa02647e8, 0xd799d053, 0xbe143d12, 0x49177474, 0xbbc0c5f4, 0x99f7fe9f, 0x24fc1559, 0xce0925cf, 0x1dded5f4,
    0x1d1a2cd3, 0xafe3ef48, 0x6fd5d075, 0x4a63bc1d, 0x93aa36c0, 0x2942d778, 0xb26a2444, 0x5616cc50, 0x7565c161,
    0xa006197b, 0xee700b07, 0x4a236a82, 0x693db870, 0x9a919e64, 0x995b05b1, 0xd4659569, 0x90e45846, 0xbca11996,
    0x3e345cd9, 0xb29a9967, 0x7e9e66f7, 0x9ce136d0, 0xcde74e76, 0xde56e4bb, 0xba4dc6ae, 0xf9d40779, 0x4e5c0bdb,
    0xde14f9e5, 0x278f8745, 0x13ce0128, 0x8bb308f5, 0x4c41a359, 0x273d1927, 0x50338e76, 0xdfceb7c2, 0xf1b86f68,
    0xc8b12d6a, 0xf4cb0e08, 0xa74b4b14, 0x81571c6a, 0xebc4a928, 0x1d6d5fd6, 0x7f4bbc87, 0x61ba542f, 0x9b06d11d,
    0xb53ae1c1, 0xdcc2a6c0, 0x7f04f8a8, 0x8da9d186, 0xa168e054, 0x21ed0ce7, 0x9ca9e9d1, 0x0e01fb38, 0xd8b6b1d9,
    0xb8d10266, 0x203a9de1, 0x37ba3ffe, 0x9fefb09f, 0x5e4cb3e2, 0xcecd03b4, 0xcc270838, 0xa1619089, 0x22995679,
    0x6dcd6b78, 0x8c50f9b1, 0x1c354ada, 0x48a0f13e, 0xca7b4696, 0x5c1fe8bf, 0xdd0f433f, 0x8aa411f1, 0x149b2ee3,
    0x181d16a1, 0x3b84b01d, 0xee745103, 0x0f230907, 0x663d1014, 0xd614181b, 0xb1b88cc9, 0x015f672c, 0x660ea636,
    0x4107c7f3, 0x6f0d8afe, 0xf0aeffeb, 0x93b25fa0, 0x620c9075, 0x155a4d7e, 0x10fdbd73, 0xb162eabe, 0xaf9605db,
    0xba35d441, 0xde327cfa, 0x15a6fd70, 0x0f2f4b54, 0xfb1b4995, 0xec092e68, 0x37ebade6, 0x850f63ca, 0xe72a879f,
    0xc823f741, 0xc6f114b8, 0x74e461f6, 0x1d01ad14, 0xfe1ed7d3, 0x306b9444, 0x9ebd40a6, 0x3275b333, 0xa8540ca1,
    0xeb8d394c, 0xa2aef54c, 0xf12d0705, 0x8974e70e, 0x59ae82cf, 0x32469aca, 0x973325d8, 0x27ba604d, 0x9aeb7827,
    0xaf0af97c, 0x9783e6f8, 0xe0725a87, 0x2f02d864, 0x717a0587, 0x0c90d7b0, 0x6828b84e, 0xba08ebe7, 0x65cf8360,
    0x63132f80, 0xbb8d4a41, 0xbd5b8b41, 0x459f019f, 0x5e68369f, 0xe855f000, 0xa79a634c, 0x172c7704, 0x07337ab3,
    0xb2926453, 0x11084c8a, 0x328689ca, 0xa7e3efcf, 0x8b9a5695, 0x76b65bbe, 0x87bb5a2a, 0x5f73e6ad, 0xcf59b265,
    0x4fe46ec9, 0x52561232, 0x70db002c, 0xc21d1b8f, 0xd7ceb1c6, 0xff4a97c8, 0xdd21c90b, 0x48c14c38, 0x64262c68,
    0x74c5d3f9, 0x66bf60e7, 0xce804348, 0x98585792, 0x7619fc86, 0x91de3f72, 0x57f5191c, 0x576d9737, 0x5f4535b0,
    0xb9ee8ef5, 0x2e9eff6c, 0xc7c9f874, 0xe6ac0843, 0xd93b8c08, 0x2f34a779, 0x407799eb, 0x2b9904e0, 0x14bb018f,
    0x1fcf367b, 0x7975c362, 0xba31448f, 0xa59286f7, 0x1255244a, 0xd685169b, 0xc791ec84, 0x3b5461b1, 0x4822924a,
    0x26d86175, 0x596e6b2f, 0x6a157bef, 0x8bc98a9b, 0xa8220343, 0x91eaad8a, 0x42b89a9e, 0x7c9b5f81, 0xb5f9ec6c,
    0xd999ef9e, 0xa547f6a3, 0xc391f010, 0xe9d8bb43};

// The following two classes (ExHDPHash and ExHDPHashComb) are used by
// the HashDistPartitioningFunction.  They are isolated from the other
// general purpose versions of these functions so that they won't be
// inadvertently altered.  Any changes these functions that results in
// a different runtime behavior will cause existing hash partitioned
// tables to be invalid.
//
// Hash Function used by Hash Partitioning. This function cannot
// change once Hash Partitioning is released!  Defined for all data
// types, returns a 32 bit non-nullable hash value for the data item.
//
class ExHDPHash : public ex_function_clause {
 public:
  ExHDPHash(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExHDPHash();

  enum {
    NO_FLAGS = 0x0000,
    SWAP_TWO = 0x0001,
    SWAP_FOUR = 0x0002,
    SWAP_EIGHT = 0x0004,
    SWAP_FIRSTTWO = 0x0008,
    SWAP_LASTFOUR = 0x0010
  };

  int isNullInNullOut() const { return 0; };
  int isNullRelevant() const { return 0; };
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  static int hash(const char *data, UInt32 flags, int length);

  static UInt64 hashP(const unsigned char *src1, const unsigned char *src2, int len1, int len2) {
    int i = 0;
    int hash1 = 0;
    int hash2 = 0;

    // Loop over all the bytes of the value and compute the hash value.
    for (; i < len1; i++) {
      // Make sure the hashValue is sensitive to the byte position.
      // One bit circular shift.
      hash1 = (hash1 << 1 | hash1 >> 31);
      hash2 = (hash2 << 1 | hash2 >> 31);

      hash1 = hash1 ^ randomHashValues[*src1++];
      hash2 = hash2 ^ randomHashValues[*src2++];
    }

    for (; i < len2; i++) {
      hash2 = (hash2 << 1 | hash2 >> 31) ^ randomHashValues[*src2++];
    }

    return (((UInt64)hash1 << 32) | hash2);
  }

  // Hash an 8 byte long key
  static int hash8(const char *data, UInt32 flags) {
    unsigned char *valp = (unsigned char *)data;
    int hashValue = 0;

    switch (flags) {
      case NO_FLAGS: {
        hashValue = randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        break;
      }
      case SWAP_TWO: {
        hashValue = randomHashValues[*(valp + 1)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 0)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 3)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 2)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 5)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 4)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 7)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 6)];
        break;
      }
      case SWAP_EIGHT: {
        valp += 8;

        hashValue = randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        break;
      }
      default:
        assert(FALSE);
        break;
    }
    return hashValue;
  }

  // Hash a 4-byte long key
  static int hash4(const char *data, UInt32 flags) {
    unsigned char *valp = (unsigned char *)data;
    int hashValue = 0;

    switch (flags) {
      case NO_FLAGS: {
        hashValue = randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        break;
      }
      case SWAP_TWO: {
        hashValue = randomHashValues[*(valp + 1)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 0)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 3)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 2)];
        break;
      }
      case SWAP_FOUR: {
        valp += 4;
        hashValue = randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        break;
      }
      default:
        assert(FALSE);
        break;
    }
    return hashValue;
  }

  // Hash a 2-byte long key
  static int hash2(const char *data, UInt32 flags) {
    unsigned char *valp = (unsigned char *)data;
    int hashValue = 0;

    switch (flags) {
      case NO_FLAGS: {
        hashValue = randomHashValues[*valp++];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
        break;
      }
      case SWAP_TWO: {
        valp += 2;
        hashValue = randomHashValues[*--valp];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*--valp];
        break;
      }
      default:
        assert(FALSE);
        break;
    }
    return hashValue;
  }

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  static const UInt32 nullHashValue = 666654765;

 private:
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 00-07
};

// This function is used to combine two hash values to produce a new
// hash value. Used by Hash Partitioning. This function cannot change
// once Hash Partitioning is released!  Defined for all data types,
// returns a 32 bit non-nullable hash value for the data item.
//
class ExHDPHashComb : public ex_function_clause {
 public:
  ExHDPHashComb(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExHDPHashComb();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_replace_null : public ex_function_clause {
 public:
  ex_function_replace_null(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_replace_null();

  int isNullInNullOut() const { return 0; };
  int isNullRelevant() const { return 0; };
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  ex_expr::exp_return_type processNulls(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 00-07
};

class ex_function_mod : public ex_function_clause {
 public:
  ex_function_mod(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_mod();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_mask : public ex_function_clause {
 public:
  ex_function_mask(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_mask();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  //  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionShift : public ex_function_clause {
 public:
  ExFunctionShift(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionShift();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  //  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_bool : public ex_function_clause {
 public:
  ex_function_bool(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_bool() {}

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_converttimestamp : public ex_function_clause {
 public:
  ex_function_converttimestamp(OperatorTypeEnum oper_type, Attributes **attr, Space *space, long gmtDiff);
  ex_function_converttimestamp();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  inline NABoolean toLocalTime(void) { return ((flags_ & TO_LOCAL_TIME) != 0); };

  inline void setToLocalTime(NABoolean v) { (v) ? flags_ |= TO_LOCAL_TIME : flags_ &= ~TO_LOCAL_TIME; };

 private:
  enum { TO_LOCAL_TIME = 0x0001 };

  int flags_;
  int filler0_;

  // difference in microseconds between gmt/utc time and local time.
  // It is +ve number for times west of gmt and -ve for times east.
  // Subtracting this number from utc time will get local time.
  long gmtDiff_;
};

class ex_function_dateformat : public ex_function_clause {
 public:
  ex_function_dateformat(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int dateformat, int num_attrs,
                         int nCaseSensitive = 0);
  ex_function_dateformat();

  void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  inline int getDateFormat() const { return dateformat_; }
  inline int getCaseSensitivity() const { return caseSensitivity_; }
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  // formats defined in class ExpDatetime in exp/exp_datetime.h
  int dateformat_;       // 00-03
  int caseSensitivity_;  // 04-07
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 08-15
};

// process to_char number format
// translate a value of NUMERIC,INT,FLOAT to varchar
class ex_function_numberformat : public ex_function_clause {
 public:
  ex_function_numberformat(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int nFormatStrLen);
  ex_function_numberformat();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  enum { MAX_NUMFORMAT_STR_LEN = 63 };

  int formatStringLen_;  // 0 - 3
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[4];  // 4 - 7
};

class ex_function_dayofweek : public ex_function_clause {
 public:
  ex_function_dayofweek(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_dayofweek();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_extract : public ex_function_clause {
 public:
  ex_function_extract(OperatorTypeEnum oper_type, Attributes **attr, Space *space, rec_datetime_field extractField);
  ex_function_extract();

  rec_datetime_field getExtractField() const { return (rec_datetime_field)extractField_; }

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  long getExtraTimeValue(rec_datetime_field eField, int eCode, char *dateTime);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  Int16 /*rec_datetime_field*/ extractField_;  // 00-01
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[6];  // 02-07
};

class ex_function_monthsbetween : public ex_function_clause {
 public:
  ex_function_monthsbetween(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_monthsbetween();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
};

class ex_function_juliantimestamp : public ex_function_clause {
 public:
  ex_function_juliantimestamp(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_juliantimestamp();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_exec_count : public ex_function_clause {
 public:
  ex_function_exec_count(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_exec_count();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  // NOTE: keeping this count in the clause means that it is shared
  // among all statements that refer to this expression (e.g.
  // in a multi-threaded environment). The only problem with this
  // is that a statement must not rely on the counts being
  // the true number of executions. Each statement will still get
  // a unique, increasing count each time it executes, but the
  // number may sometimes increase by more than 1 between executions.
  long execCount_;  // 00-07
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 08-15
};

class ex_function_curr_transid : public ex_function_clause {
 public:
  ex_function_curr_transid(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_curr_transid();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

// Helper routines used by ansi_user::eval and MXUDR code to obtain
// CURRENT_USER or SESSION_USER value. Not used by DP2.

short exp_function_get_user(OperatorTypeEnum user_type, char *userNameBuffer, int inputBufferLength,
                            int *actualLength);  // OUT optional

class ex_function_ansi_user : public ex_function_clause {
 public:
  ex_function_ansi_user(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_ansi_user();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

short exp_function_get_tenant(OperatorTypeEnum user_type, char *userNameBuffer, int inputBufferLength,
                              int &actualLength);

class ex_function_ansi_tenant : public ex_function_clause {
 public:
  ex_function_ansi_tenant(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_ansi_tenant();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_user : public ex_function_clause {
 public:
  ex_function_user(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_user();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExpRaiseErrorFunction : public ex_function_clause {
 public:
  ExpRaiseErrorFunction(Attributes **attr, Space *space, int sqlCode, NABoolean raiseError = TRUE,
                        const char *constraintName = NULL, const char *tableName = NULL,
                        const NABoolean hasStringExp = FALSE,  // -- Triggers
                        const char *optionalStr = NULL);
  ExpRaiseErrorFunction();

  // isNullInNullOut() const { return 0;};
  // isNullRelevant() const {return 0;};
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  int unpack(void *, void *reallocator);

  int getSQLCODE() { return theSQLCODE_; };
  const char *getConstraintName() { return constraintName_; };
  void setConstraintName(const char *constraintName) { constraintName_ = (char *)constraintName; };
  const char *getTableName() { return tableName_; };
  void setTableName(const char *tableName) { tableName_ = (char *)tableName; };

  const char *getOptionalStr() { return optionalStr_; }

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  inline NABoolean raiseError(void) { return ((flags_ & RAISE_ERROR) != 0); };

  inline void setRaiseError(NABoolean v) { (v) ? flags_ |= RAISE_ERROR : flags_ &= ~RAISE_ERROR; };

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  enum {
    RAISE_ERROR = 0x00000001  // Raise Error if set, or it's warning
  };

  enum { MAX_OPTIONAL_STR_LEN = 1023 };

  NABasicPtr /*const char* */ constraintName_;  // 00-07
  NABasicPtr /*const char* */ tableName_;       // 08-15
  int theSQLCODE_;                              // 16-19
  int flags_;                                   // 20-23
  // TRUE, raise error. FALSE, raise warning.

  // one byte for null terminator.
  char optionalStr_[MAX_OPTIONAL_STR_LEN + 1];  // 24-1047

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 1048-1055
};

class ExFunctionSVariance : public ex_function_clause {
 public:
  ExFunctionSVariance(Attributes **attr, Space *space);
  ExFunctionSVariance(OperatorTypeEnum oper_type, Attributes **attr, Space *space);

  ExFunctionSVariance();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);
  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionSStddev : public ex_function_clause {
 public:
  ExFunctionSStddev(Attributes **attr, Space *space);
  ExFunctionSStddev(OperatorTypeEnum oper_type, Attributes **attr, Space *space);

  ExFunctionSStddev();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

// class ExUnPackCol ----------------------------------------------
// This function clause implements the UnPackCol ItemExpr.  It is used
// to extract column values from a packed row.  The data members of
// this class are:
//
//   long width_: The width of the data to be extracted (in BITS).
//
//   long base_: The offset of the Data section of the packed row (in BYTES).
//
//   nullsPresent_: If TRUE, a NULL Bitmap section of the packed row is
//   present.
//
// This node has two children. The first is the source packed row and is
// of type char(n) not null, the second is the index into the packed row
// and is of type int not null.
//
class ExUnPackCol : public ex_function_clause {
 public:
  ExUnPackCol(Attributes **attr, Space *space, int width, int base, NABoolean nullsPresent);

  ExUnPackCol();

  // Null Semantics
  //
  // NullInNullOut also implies, NotNullInNotNullOut. Since
  // this clause does not have NULL in, but can have NULL out,
  // This method must return 0.
  //
  int isNullInNullOut() const { return 0; };

  // This clause handles all NULL processing in the eval()
  // method.
  //
  int isNullRelevant() const { return 0; };

  // This is where all the work is done.
  //
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // This is a different kind of packing, done to relocate the
  // expressions.
  //
  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  inline NABoolean nullsPresent(void) const { return ((flags_ & NULLS_PRESENT) != 0); };

  inline void setNullsPresent(NABoolean v) { (v) ? flags_ |= NULLS_PRESENT : flags_ &= ~NULLS_PRESENT; };

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  enum {
    NULLS_PRESENT = 0x00000001  // Nulls present if set
  };

  // The width of the data to be extracted. (in BITS).
  //
  int width_;  // 00-03

  // The offset to the Data section of the packed row (in BYTES).
  //
  int base_;  // 04-07

  // If TRUE, a NULL Bitmap is present in the packed row.
  //
  int flags_;  // 08-11
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[4];  // 12-15
};

class ExFunctionRandomNum : public ex_function_clause {
 public:
  ExFunctionRandomNum(OperatorTypeEnum opType, short num_operands, NABoolean simpleRandom, NABoolean useThreadIdForSeed,
                      Attributes **attr, Space *space);

  ExFunctionRandomNum();

  void genRand(char *op_data[]);

  int getRand() { return seed_; };

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  // ---------------------------------------------------------------------

 private:
  enum { SIMPLE_RANDOM = 0x0001 };

  void initSeed(char *op_data[]);

  NABoolean simpleRandom() { return (flags_ & SIMPLE_RANDOM) != 0; }

  int seed_;  // 00-03

  Int16 flags_;  // 04-05
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[2];  // 06-07
};

class ExFunctionRandomSelection : public ExFunctionRandomNum {
 public:
  ExFunctionRandomSelection(OperatorTypeEnum opType, Attributes **attr, Space *space, float selProb);

  ExFunctionRandomSelection();

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  Long pack(void *);

 private:
  // TODO: when ready to require dp2 rebuild
  // virtual short getClassSize() { return (short)sizeof(*this); }

  void initDiff();

  Float32 selProbability_;  // 00-03
  int difference_;          // 04-07
  int normProbability_;     // 08-11

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[12];  // 12-23
};

// MV,
class ExFunctionGenericUpdateOutput : public ex_function_clause {
 public:
  ExFunctionGenericUpdateOutput(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionGenericUpdateOutput();
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *space);
  virtual short getClassSize() { return (short)sizeof(*this); }
};

// ++ Triggers -
class ExFunctionInternalTimestamp : public ex_function_clause {
 public:
  ExFunctionInternalTimestamp(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionInternalTimestamp();
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *space);
};

class ExHash2Distrib : public ex_function_clause {
 public:
  ExHash2Distrib(Attributes **attr, Space *space);

  ExHash2Distrib();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);
  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExProgDistrib : public ex_function_clause {
 public:
  ExProgDistrib(Attributes **attr, Space *space);

  ExProgDistrib();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExProgDistribKey : public ex_function_clause {
 public:
  ExProgDistribKey(Attributes **attr, Space *space);

  ExProgDistribKey();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExPAGroup : public ex_function_clause {
 public:
  ExPAGroup(Attributes **attr, Space *space);

  ExPAGroup();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionPack : public ex_function_clause {
 public:
  ExFunctionPack(Attributes **attr, Space *space, int width, int base, NABoolean nullsPresent);

  ExFunctionPack();

  int isNullInNullOut() const { return 0; }

  // Processing of null is in eval().
  int isNullRelevant() const { return 0; }

  int isEvalRelevant() const { return 1; }

  // ex_expr::exp_return_type processNulls(char *op_data[],
  //						      CollHeap*,
  //						      ComDiagsArea** = 0);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  inline NABoolean nullsPresent(void) { return ((flags_ & NULLS_PRESENT) != 0); };

  inline void setNullsPresent(NABoolean v) { (v) ? flags_ |= NULLS_PRESENT : flags_ &= ~NULLS_PRESENT; };

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  enum {
    NULLS_PRESENT = 0x00000001  // Nulls present if set
  };

  int width_;  // 00-03
  int base_;   // 04-07
  int flags_;  // 08-11
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[4];  // 12-15
};

class ExFunctionRangeLookup : public ex_function_clause {
 public:
  ExFunctionRangeLookup(Attributes **attr, Space *space, int numParts, int partKeyLen);

  ExFunctionRangeLookup();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int numParts_;    // 00-03
  int partKeyLen_;  // 04-07

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 08-15
};

// class ExRowsetArrayScan
// The ExRowsetArrayScan class is the physical implementation of the
// RowsetArrayScan node which is used to extract an element of a Rowset
// array.
// Items of this type are attached to the UnpackRows node at bind time.
// When the generator finds an UnpackRows node, it creates an
// PhysUnPackRows node, and when it finds an RowsetArrayScan item, it
// generates a ExRowsetArrayScan node. At run time, each ExRowsetArrayScan
// node is evaluated and the corresponding element is returned by the
// PhysUnPackRows node to its parent.
// This class is derived from the ex_function_clause and has two children.
// The first child is the host variable array and the second child is
// the host variable used at execution time to index the array.

class ExRowsetArrayScan : public ex_function_clause {
 public:
  ExRowsetArrayScan(Attributes **attr, Space *space, int maxNumElem, int elemSize, NABoolean elemNullInd,
                    NABoolean optLargVar);

  ExRowsetArrayScan();

  // This clause handles all NULL processing in the eval()
  // method.
  //
  int isNullRelevant() const { return 0; };
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);
  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int maxNumElem_;   // Maximum number of elements          // 00-03
  int elemSize_;     // Element storage length in bytes     // 04-07
  int elemNullInd_;  // Null Present ?                      // 08-11
  int optLargVar_;   // 12-15
};

class ExRowsetArrayRowid : public ex_function_clause {
 public:
  ExRowsetArrayRowid(Attributes **attr, Space *space, int maxNumElem);

  ExRowsetArrayRowid();

  // This clause handles all NULL processing in the eval()
  // method.
  //
  int isNullRelevant() const { return 0; };
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int maxNumElem_;         // Maximum number of elements          // 00-03
  char fillersRowset_[4];  // 04-07
};

class ExRowsetArrayInto : public ex_function_clause {
 public:
  ExRowsetArrayInto(Attributes **attr, Space *space, int maxNumElem, int elemSize, NABoolean elemNullInd);

  ExRowsetArrayInto();

  // This clause handles all NULL processing in the eval()
  // method.
  //
  int isNullRelevant() const { return 0; };
  int isEvalRelevant() const { return 1; }
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  // ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  virtual int getElemSize() { return elemSize_; }

  virtual int getElemNullInd() { return elemNullInd_; }

  // ---------------------------------------------------------------------

 private:
  int maxNumElem_;   // Maximum number of elements          // 00-03
  int numElem_;      // Actual number of elements           // 04-07
  int elemSize_;     // Element storage length in bytes     // 08-11
  int elemNullInd_;  // Null Present ?                      // 12-15

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 16-23
};

class ex_function_nullifzero : public ex_function_clause {
 public:
  ex_function_nullifzero(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_nullifzero();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

class ex_function_nvl : public ex_function_clause {
 public:
  ex_function_nvl(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_nvl();

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

  // This clause handles all NULL processing in the eval() method.
  int isNullRelevant() const { return 0; };

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

// get first ngram string
class ex_function_firstngram : public ex_function_clause {
 public:
  ex_function_firstngram(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_firstngram();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

  // This clause handles all NULL processing in the eval() method.
  int isNullRelevant() const { return 0; };

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

class ex_function_ngramcount : public ex_function_clause {
 public:
  ex_function_ngramcount(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_ngramcount();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

  // This clause handles all NULL processing in the eval() method.
  int isNullRelevant() const { return 0; };

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

class ex_function_queryid_extract : public ex_function_clause {
 public:
  ex_function_queryid_extract(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_queryid_extract();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

class ExFunctionUniqueId : public ex_function_clause {
 public:
  ExFunctionUniqueId(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionUniqueId();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

class ExFunctionRowNum : public ex_function_clause {
 public:
  ExFunctionRowNum(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionRowNum();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  char fillers_[64];
  // ---------------------------------------------------------------------
};

class ExFunctionHbaseColumnLookup : public ex_function_clause {
 public:
  ExFunctionHbaseColumnLookup(OperatorTypeEnum oper_type, Attributes **attr, const char *colName, Space *space);
  ExFunctionHbaseColumnLookup();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

  // This clause handles all NULL processing in the eval() method.
  int isNullRelevant() const { return 0; };

  static short extractColFamilyAndName(const char *input, short len, NABoolean isVarchar, std::string &colFam,
                                       std::string &colName);

 private:
  char colName_[256];
  // ---------------------------------------------------------------------
};

class ExFunctionHbaseColumnsDisplay : public ex_function_clause {
 public:
  ExFunctionHbaseColumnsDisplay(OperatorTypeEnum oper_type, Attributes **attr, int numCols, char *colNames,
                                Space *space);
  ExFunctionHbaseColumnsDisplay();

  void init(OperatorTypeEnum oper_type, Attributes **attr, int numCols, char *colNames, Space *space);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  int unpack(void *base, void *reallocator);

  virtual short getClassSize() { return (short)sizeof(*this); }

  char *colNames() { return colNames_; }

 private:
  NABoolean toBeDisplayed(char *colName, int colNameLen);

  int numCols_;
  char filler1_[4];
  NABasicPtr colNames_;

  // ---------------------------------------------------------------------
};

class ExFunctionHbaseColumnCreate : public ex_function_clause {
 public:
  ExFunctionHbaseColumnCreate(OperatorTypeEnum oper_type, Attributes **attr, short numEntries, short colNameMaxLen,
                              int colValMaxLen, short colValVCIndLen, Space *space);
  ExFunctionHbaseColumnCreate();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  int unpack(void *base, void *reallocator);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  int flags_;

  short numEntries_;
  short colNameMaxLen_;
  int colValMaxLen_;
  short colValVCIndLen_;  // 2 or 4 bytes
  char filler1_[2];
};

class ExFunctionCastType : public ex_function_clause {
 public:
  ExFunctionCastType(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionCastType();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  //  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
};

class ExFunctionSequenceValue : public ex_function_clause {
 public:
  ExFunctionSequenceValue(OperatorTypeEnum oper_type, Attributes **attr, const SequenceGeneratorAttributes &sga,
                          Space *space);
  ExFunctionSequenceValue();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  //  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

  // This clause handles all NULL processing in the eval() method.
  int isNullRelevant() const { return 0; };

  void setIsCurr(NABoolean v) { (v) ? flags_ |= IS_CURR : flags_ &= ~IS_CURR; };

  void setUseDlockImpl(NABoolean v) { (v) ? flags_ |= USE_DLOCK_IMPL : flags_ &= ~USE_DLOCK_IMPL; };

  void setSkipWalForIncrColVal(NABoolean v) {
    (v) ? flags_ |= SKIP_WAL_FOR_INCRCOLVAL : flags_ &= ~SKIP_WAL_FOR_INCRCOLVAL;
  };

  void setUseDtmImpl(NABoolean v) { (v) ? flags_ |= USE_DTM_IMPL : flags_ &= ~USE_DTM_IMPL; };

  NABoolean isCurr() { return ((flags_ & IS_CURR) != 0); }

  NABoolean useDlockImpl() { return ((flags_ & USE_DLOCK_IMPL) != 0); }

  NABoolean isSkipWalForIncrColVal() { return ((flags_ & SKIP_WAL_FOR_INCRCOLVAL) != 0); }

  NABoolean useDtmImpl() { return ((flags_ & USE_DTM_IMPL) != 0); }

  void setRetryNum(UInt32 n) { retryNum_ = n; }

  UInt32 getRetryNum() { return retryNum_; }

 private:
  enum {
    IS_CURR = 0x0001,
    USE_DLOCK_IMPL = 0x0002,
    USE_DTM_IMPL = 0x0004,
    SKIP_WAL_FOR_INCRCOLVAL = 0x0008,
  };

  SequenceGeneratorAttributes sga_;

  UInt32 flags_;

  UInt32 retryNum_;
  // ---------------------------------------------------------------------
};

class ExFunctionHbaseVisibility : public ex_function_clause {
 public:
  ExFunctionHbaseVisibility(OperatorTypeEnum oper_type, Attributes **attr, int tagType, int colIndex, Space *space);
  ExFunctionHbaseVisibility();

  void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  int colIndex_;
  int tagType_;
  int flags_;
  int filler_;
  // ---------------------------------------------------------------------
};

class ExFunctionHbaseVisibilitySet : public ex_function_clause {
 public:
  enum { MAX_VIS_EXPR_LEN = 1024 };

  ExFunctionHbaseVisibilitySet(OperatorTypeEnum oper_type, Attributes **attr, short colIDlen, const char *colID,
                               int visExprLen, const char *visExpr, Space *space);
  ExFunctionHbaseVisibilitySet();

  void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual short getClassSize() { return (short)sizeof(*this); }

  char *visExpr() { return visExpr_; }

 private:
  short colIDlen_;
  char colID_[6];

  UInt32 flags_;

  int visExprLen_;

  char visExpr_[MAX_VIS_EXPR_LEN];

  // ---------------------------------------------------------------------
};

class ExFunctionHbaseTimestamp : public ex_function_clause {
 public:
  ExFunctionHbaseTimestamp(OperatorTypeEnum oper_type, Attributes **attr, int colIndex, Space *space);
  ExFunctionHbaseTimestamp();

  void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  int colIndex_;
  int flags_;
  // ---------------------------------------------------------------------
};

class ExFunctionHbaseVersion : public ex_function_clause {
 public:
  ExFunctionHbaseVersion(OperatorTypeEnum oper_type, Attributes **attr, int colIndex, Space *space);
  ExFunctionHbaseVersion();

  void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  int colIndex_;
  int flags_;
  // ---------------------------------------------------------------------
};

class ExFunctionHbaseRowid : public ex_function_clause {
 public:
  ExFunctionHbaseRowid(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ExFunctionHbaseRowid();

  void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual short getClassSize() { return (short)sizeof(*this); }

  // ---------------------------------------------------------------------
};

class ExHeaderClause : public ex_function_clause {
 public:
  ExHeaderClause() {}

  ExHeaderClause(Attributes **attr, Space *space, UInt16 adminSize, UInt16 entryOffset, UInt16 bitmapOffset,
                 UInt16 firstFixedOffset)
      : ex_function_clause(ITM_HEADER, 1, attr, space),
        adminSz_(adminSize),
        entryOffset_(entryOffset),
        bitmapOffset_(bitmapOffset),
        firstFixedOffset_(firstFixedOffset) {}

  virtual ~ExHeaderClause() {}

  // Null values aren't relevant for this clause.
  int isNullRelevant() const { return 0; };

  // This is where all the work is done.
  //
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea = 0);

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 flags);

  // This is a different kind of packing, done to relocate the
  // expressions.
  //
  Long pack(void *space);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  NABoolean isSQLMXAlignedFormat() { return (entryOffset_ > 0); }

  // ---------------------------------------------------------------------
 private:
  // The complete admin size - header, complete VOA, bitmap, any padding before
  // the first fixed field.
  UInt16 adminSz_;  // 00-01

  // Actual offset value to where the bitmap resides within the data record.
  // This value is the number of bytes to the start of the bitmap.
  // This value may be 0 if no null columns are present (bitmapSize_ is 0 too),
  // OR if this expression clause is used for SQL
  UInt16 bitmapOffset_;  // 02-03

  // Where in the header the bitmap offset value is written.
  UInt16 entryOffset_;  // 04-05

  // Offset to the first fixed field (may be 0).
  UInt16 firstFixedOffset_;  // 06-07

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[8];  // 08-15
};

class ex_function_split_part : public ex_function_clause {
 public:
  ex_function_split_part(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_split_part();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);
  virtual unsigned char getClassVersionID() { return 1; }
  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
};

class ex_function_beginkey : public ex_function_clause {
 public:
  ex_function_beginkey(OperatorTypeEnum oper_type, Attributes **attr, Space *space);
  ex_function_beginkey();
  ~ex_function_beginkey();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ex_function_endkey : public ex_function_clause {
 public:
  ex_function_endkey(OperatorTypeEnum oper_type, Attributes **attr, Int16 bytesPerChar, NAWchar maxValue, Space *space);
  ex_function_endkey();
  ~ex_function_endkey();

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);
  Long pack(void *);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
  Int16 bytesPerChar_;
  NAWchar maxValue_;
  // ---------------------------------------------------------------------
};

#ifndef ULONG
#define ULONG int
#endif

#endif
