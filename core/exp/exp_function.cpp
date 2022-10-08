/*********************************************************************

**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include <math.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <unistd.h>
#include <zlib.h>

#include "ComSSL.h"
#include "common/Platform.h"
#define MathSqrt(op, err) sqrt(op)

#include <ctype.h>
#include <regex.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <time.h>
#include <uuid/uuid.h>

#include "common/SQLTypeDefs.h"
#include "common/ComAnsiNamePart.h"
#include "common/ComCextdecs.h"
#include "common/ComDefs.h"
#include "common/ComEncryption.h"
#include "common/ComRtUtils.h"
#include "common/ComSqlId.h"
#include "common/ComSysUtils.h"
#include "common/ComUser.h"
#include "common/NAUserId.h"
#include "common/NLSConversion.h"
#include "common/nawstring.h"
#include "common/ngram.h"
#include "common/wstr.h"
#include "executor/ex_globals.h"
#include "exp/ExpSeqGen.h"
#include "exp/exp_bignum.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_datetime.h"
#include "exp/exp_interval.h"
#include "exp/exp_like.h"
#include "exp/exp_stdh.h"
#include "exp/exp_function.h"
#include "export/ComDiags.h"
#include "qmscommon/QRLogger.h"
#include "seabed/sys.h"
#undef DllImport
#define DllImport __declspec(dllimport)
#include "rosetta/rosgen.h"

#define ptimez_h_juliantimestamp
#define ptimez_h_including_section
#include "guardian/ptimez.h"
#ifdef ptimez_h_juliantimestamp
Section missing,
    generate compiler error
#endif

#define ptimez_h_converttimestamp
#define ptimez_h_including_section
#include "guardian/ptimez.h"
#ifdef ptimez_h_converttimestamp
        Section missing,
    generate compiler error
#endif

#define ptimez_h_interprettimestamp
#define ptimez_h_including_section
#include "guardian/ptimez.h"
#ifdef ptimez_h_interprettimestamp
        Section missing,
    generate compiler error
#endif

#define ptimez_h_computetimestamp
#define ptimez_h_including_section
#include "guardian/ptimez.h"
#ifdef ptimez_h_computetimestamp
        Section missing,
    generate compiler error
#endif

#define psecure_h_including_section
#define psecure_h_security_app_priv_
#define psecure_h_security_psb_get_
#define psecure_h_security_ntuser_set_
#include "security/psecure.h"
#ifndef dsecure_h_INCLUDED
#define dsecure_h_INCLUDED
#include "security/dsecure.h"
#endif
#include "common/feerrors.h"
#include "exp_numberformat.h"
#include "float.h"
#include "security/uid.h"

    extern char *
    exClauseGetText(OperatorTypeEnum ote);

void setVCLength(char *VCLen, int VCLenSize, int value);

static void ExRaiseJSONError(CollHeap *heap, ComDiagsArea **diagsArea, JsonReturnType type);

static void regexpReplaceCleanup(regex_t *reg, char *s1, char *s2, char *s3, char *s4, CollHeap *heap);

static int regexpReplaceBuildReplaceStr(char *repPattern, int repPatternLen, char *replaceStr, int &replaceStrLen,
                                        char *srcStr, regmatch_t *pm, const size_t nmatch, ComDiagsArea **diagsArea,
                                        CollHeap *heap);

//#define TOUPPER(c) (((c >= 'a') && (c <= 'z')) ? (c - 32) : c);
//#define TOLOWER(c) (((c >= 'A') && (c <= 'Z')) ? (c + 32) : c);

// -----------------------------------------------------------------------
// There is currently a bug in the tandem include file sys/time.h that
// prevents us to get the definition of gettimeofday from there.
// -----------------------------------------------------------------------
// extern int  gettimeofday(struct timeval *, struct timezone *);

ExFunctionAscii::ExFunctionAscii(){};
ExFunctionMaxBytes::ExFunctionMaxBytes(){};
ExFunctionChar::ExFunctionChar(){};
ExFunctionConvertHex::ExFunctionConvertHex(){};
ExFunctionRepeat::ExFunctionRepeat(){};
ExFunctionReplace::ExFunctionReplace() {
  collation_ = CharInfo::DefaultCollation;
  setArgEncodedLen(0, 0);  // initialize the first child encoded length to 0
  setArgEncodedLen(0, 1);  // initialize the second child encoded length to 0
};
ex_function_char_length::ex_function_char_length(){};
ex_function_char_length_doublebyte::ex_function_char_length_doublebyte(){};
ex_function_oct_length::ex_function_oct_length(){};
ex_function_position::ex_function_position(){};
ex_function_concat::ex_function_concat(){};
ex_function_lower::ex_function_lower(){};
ex_function_upper::ex_function_upper(){};
ex_function_substring::ex_function_substring(){};
ex_function_trim_char::ex_function_trim_char(){};
ExFunctionTokenStr::ExFunctionTokenStr(){};
ExFunctionReverseStr::ExFunctionReverseStr(){};
ex_function_current::ex_function_current(){};
ex_function_unixtime::ex_function_unixtime(){};
ex_function_sleep::ex_function_sleep(){};
ex_function_unique_execute_id::ex_function_unique_execute_id(){};      // Trigger -
ex_function_get_triggers_status::ex_function_get_triggers_status(){};  // Trigger -
ex_function_get_bit_value_at::ex_function_get_bit_value_at(){};        // Trigger -
ex_function_is_bitwise_and_true::ex_function_is_bitwise_and_true(){};  // MV
ex_function_explode_varchar::ex_function_explode_varchar(){};
ex_function_hash::ex_function_hash(){};
ex_function_hivehash::ex_function_hivehash(){};
ExHashComb::ExHashComb(){};
ExHiveHashComb::ExHiveHashComb(){};
ExHDPHash::ExHDPHash(){};
ExHDPHashComb::ExHDPHashComb(){};
ex_function_replace_null::ex_function_replace_null(){};
ex_function_mod::ex_function_mod(){};
ex_function_beginkey::ex_function_beginkey(){};
ex_function_endkey::ex_function_endkey(){};
ex_function_mask::ex_function_mask(){};
ExFunctionShift::ExFunctionShift(){};
ex_function_converttimestamp::ex_function_converttimestamp(){};
ex_function_dateformat::ex_function_dateformat(){};
ex_function_numberformat::ex_function_numberformat(){};
ex_function_dayofweek::ex_function_dayofweek(){};
ex_function_extract::ex_function_extract(){};
ex_function_monthsbetween::ex_function_monthsbetween(){};
ex_function_juliantimestamp::ex_function_juliantimestamp(){};
ex_function_exec_count::ex_function_exec_count(){};
ex_function_curr_transid::ex_function_curr_transid(){};
ex_function_ansi_user::ex_function_ansi_user(){};
ex_function_ansi_tenant::ex_function_ansi_tenant(){};
ex_function_user::ex_function_user(){};
ex_function_nullifzero::ex_function_nullifzero(){};
ex_function_nvl::ex_function_nvl(){};
// for ngram
ex_function_firstngram::ex_function_firstngram(){};
ex_function_ngramcount::ex_function_ngramcount(){};

ex_function_split_part::ex_function_split_part(){};

ex_function_queryid_extract::ex_function_queryid_extract(){};
ExFunctionUniqueId::ExFunctionUniqueId(){};
ExFunctionRowNum::ExFunctionRowNum(){};
ExFunctionHbaseColumnLookup::ExFunctionHbaseColumnLookup(){};
ExFunctionHbaseColumnsDisplay::ExFunctionHbaseColumnsDisplay(){};
ExFunctionHbaseColumnCreate::ExFunctionHbaseColumnCreate(){};
ExFunctionCastType::ExFunctionCastType(){};
ExFunctionSequenceValue::ExFunctionSequenceValue(){};
ExFunctionHbaseVisibility::ExFunctionHbaseVisibility(){};
ExFunctionHbaseVisibilitySet::ExFunctionHbaseVisibilitySet(){};
ExFunctionHbaseTimestamp::ExFunctionHbaseTimestamp(){};
ExFunctionHbaseVersion::ExFunctionHbaseVersion(){};
ExFunctionHbaseRowid::ExFunctionHbaseRowid(){};
ExFunctionSVariance::ExFunctionSVariance(){};
ExFunctionSStddev::ExFunctionSStddev(){};
ExpRaiseErrorFunction::ExpRaiseErrorFunction(){};
ExFunctionRandomNum::ExFunctionRandomNum(){};
ExFunctionGenericUpdateOutput::ExFunctionGenericUpdateOutput(){};  // MV,
ExFunctionInternalTimestamp::ExFunctionInternalTimestamp(){};      // Triggers
ExFunctionRandomSelection::ExFunctionRandomSelection(){};
ExHash2Distrib::ExHash2Distrib(){};
ExProgDistrib::ExProgDistrib(){};
ExProgDistribKey::ExProgDistribKey(){};
ExPAGroup::ExPAGroup(){};
ExFunctionPack::ExFunctionPack(){};
ExUnPackCol::ExUnPackCol(){};
ExFunctionRangeLookup::ExFunctionRangeLookup(){};
ExFunctionCrc32::ExFunctionCrc32(){};
ExFunctionMd5::ExFunctionMd5(){};
ExFunctionSha::ExFunctionSha(){};
ExFunctionSha2::ExFunctionSha2(){};
ExFunctionIsIP::ExFunctionIsIP(){};
ExFunctionInetAton::ExFunctionInetAton(){};
ExFunctionInetNtoa::ExFunctionInetNtoa(){};
ExFunctionSoundex::ExFunctionSoundex(){};
ExFunctionAESEncrypt::ExFunctionAESEncrypt(){};
ExFunctionAESDecrypt::ExFunctionAESDecrypt(){};
ExFunctionBase64EncDec::ExFunctionBase64EncDec(){};

ExFunctionAscii::ExFunctionAscii(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionMaxBytes::ExFunctionMaxBytes(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ExFunctionChar::ExFunctionChar(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionCrc32::ExFunctionCrc32(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionMd5::ExFunctionMd5(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionSha::ExFunctionSha(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionSha2::ExFunctionSha2(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode)
    : ex_function_clause(oper_type, 2, attr, space),
      mode(mode){

      };

ExFunctionIsIP::ExFunctionIsIP(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionInetAton::ExFunctionInetAton(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionInetNtoa::ExFunctionInetNtoa(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionAESEncrypt::ExFunctionAESEncrypt(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int in_args_num,
                                           int aes_mode)
    : ex_function_clause(oper_type, in_args_num + 1, attr, space), args_num(in_args_num), aes_mode(aes_mode){};

ExFunctionAESDecrypt::ExFunctionAESDecrypt(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int in_args_num,
                                           int aes_mode)
    : ex_function_clause(oper_type, in_args_num + 1, attr, space), args_num(in_args_num), aes_mode(aes_mode){};

ExFunctionBase64EncDec::ExFunctionBase64EncDec(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                               NABoolean isEncode, int maxBuflen)
    : ex_function_clause(oper_type, 2, attr, space), isEncode_(isEncode), maxBuflen_(maxBuflen), decodedBuf_(NULL){};

ExFunctionConvertHex::ExFunctionConvertHex(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ExFunctionRepeat::ExFunctionRepeat(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ExFunctionReplace::ExFunctionReplace(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 4, attr, space) {
  collation_ = CharInfo::DefaultCollation;
  // set first and second child encoded length
  setArgEncodedLen(0, 0);  // initialize the first child encoded length to 0
  setArgEncodedLen(0, 1);  // initialize the second child encoded length to 0
};

// If the argument of ex_clause is NULL and Nullable, zero it.
// il is the list of 0 based argument index.Please use {} to include them
// e.g.
// clearNullValue(op_data,attributes,{1,2})
// means the first and the second argument need to be zeroed.
static void clearNullValue(char *op_data[], AttributesPtr *attributes, initializer_list<int> il) {
  for (auto ptr = il.begin(); ptr != il.end(); ptr++) {
    int nIdx = *ptr;
    if (attributes[nIdx]->getNullFlag() && !op_data[nIdx]) {
      ExpTupleDesc::clearNullValue(op_data[0], attributes[0]->getNullBitIndex(), attributes[0]->getTupleFormat());
    }
  }
}

// Move null value to result.
// We should set varLength to 0 when we call ExpTupleDesc::setNullValue
static NABoolean setNullValue(char *op_data[], AttributesPtr *attributes) {
  NABoolean bRet = FALSE;
  if (!op_data || !attributes) return bRet;
  Attributes *tgtAttr = attributes[0];
  if (!tgtAttr) return bRet;

  char *rslt = op_data[0];

  if (!rslt) return bRet;

  if (tgtAttr->getNullFlag()) {
    ExpTupleDesc::setNullValue(op_data[0], tgtAttr->getNullBitIndex(), tgtAttr->getTupleFormat());
    tgtAttr->setVarLength(0, op_data[ex_clause::MAX_OPERANDS]);
    bRet = TRUE;
  }
  return bRet;
}

ex_expr::exp_return_type ExFunctionReplace::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // If the first arg is NULL, the result is NULL.
  if (getOperand(1)->getNullFlag() && !op_data[1]) {
    setNullValue(op_data, getOperand());
    return ex_expr::EXPR_NULL;
  }
  // If the second and the third arg is NULL, zero it and call eval
  clearNullValue(op_data, getOperand(), {2, 3});

  if (getOperand(0)->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }
  return ex_expr::EXPR_OK;
};

ExRegexpReplace::ExRegexpReplace(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, num_operands, attr, space) {}

ex_expr::exp_return_type ExRegexpReplace::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // If the first arg is NULL, the result is NULL.
  if (getOperand(1)->getNullFlag() && !op_data[1]) {
    setNullValue(op_data, getOperand());
    return ex_expr::EXPR_NULL;
  }
  // If the second and the third arg is NULL, zero it and call eval
  clearNullValue(op_data, getOperand(), {2, 3});

  if (getOperand(0)->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }
  return ex_expr::EXPR_OK;
};

ExRegexpSubstrOrCount::ExRegexpSubstrOrCount(OperatorTypeEnum oper_type, short num_operands, Attributes **attr,
                                             Space *space, NABoolean bIsSubstr)
    : ex_function_clause(oper_type, num_operands, attr, space) {
  bIsSubstr_ = bIsSubstr;
}

ex_function_char_length::ex_function_char_length(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_char_length_doublebyte::ex_function_char_length_doublebyte(OperatorTypeEnum oper_type, Attributes **attr,
                                                                       Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ex_function_oct_length::ex_function_oct_length(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_position::ex_function_position(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int in_args_num)
    : ex_function_clause(oper_type, in_args_num, attr, space),
      flags_(0){

      };

ex_function_concat::ex_function_concat(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ex_expr::exp_return_type ex_function_concat::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  if ((getOperand(1)->getNullFlag() && !op_data[1]) && (getOperand(2)->getNullFlag() && !op_data[2])) {
    setNullValue(op_data, getOperand());
    return ex_expr::EXPR_NULL;
  }
  // If the first and the second arg is NULL, zero it and call eval
  clearNullValue(op_data, getOperand(), {1, 2});

  if (getOperand(0)->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }
  return ex_expr::EXPR_OK;
};

ex_function_lower::ex_function_lower(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_upper::ex_function_upper(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_substring::ex_function_substring(OperatorTypeEnum oper_type, short num_operands, Attributes **attr,
                                             Space *space)
    : ex_function_clause(oper_type, num_operands, attr, space){

      };

ex_expr::exp_return_type ex_function_substring::processNulls(char *op_data[], CollHeap *heap,
                                                             ComDiagsArea **diagsArea) {
  // If one of the arg is NULL, the result is NULL.
  if ((getOperand(1)->getNullFlag() && !op_data[1]) || (getOperand(2)->getNullFlag() && !op_data[2])) {
    setNullValue(op_data, getOperand());
    return ex_expr::EXPR_NULL;
  }
  if (getNumOperands() == 4) {
    // If the third arg is NULL, zero it and call eval
    clearNullValue(op_data, getOperand(), {3});
  }
  if (getOperand(0)->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }
  return ex_expr::EXPR_OK;
};

ex_function_translate::ex_function_translate(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int conv_type,
                                             Int16 flags)
    : ex_function_clause(oper_type, 2, attr, space) {
  conv_type_ = conv_type;
  flags_ = flags;
};

ex_function_trim::ex_function_trim(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode)
    : ex_function_clause(oper_type, 3, attr, space) {
  mode_ = mode;
};

ex_function_trim_char::ex_function_trim_char(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int mode)
    : ex_function_trim(oper_type, attr, space, mode){};

ex_expr::exp_return_type ex_function_trim_char::processNulls(char *op_data[], CollHeap *heap,
                                                             ComDiagsArea **diagsArea) {
  // If the first arg is NULL, the result is NULL.
  if (getOperand(2)->getNullFlag() && !op_data[2]) {
    setNullValue(op_data, getOperand());
    return ex_expr::EXPR_NULL;
  }

  if (getOperand(0)->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }
  return ex_expr::EXPR_OK;
};

ExFunctionTokenStr::ExFunctionTokenStr(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, num_operands, attr, space){};

ExFunctionReverseStr::ExFunctionReverseStr(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ex_function_current::ex_function_current(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

ex_function_sleep::ex_function_sleep(OperatorTypeEnum oper_type, short numOperands, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, numOperands, attr, space){

      };

ex_function_unixtime::ex_function_unixtime(OperatorTypeEnum oper_type, short numOperands, Attributes **attr,
                                           Space *space)
    : ex_function_clause(oper_type, numOperands, attr, space){

      };
//++ Triggers -
ex_function_unique_execute_id::ex_function_unique_execute_id(OperatorTypeEnum oper_type, Attributes **attr,
                                                             Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

//++ Triggers -
ex_function_get_triggers_status::ex_function_get_triggers_status(OperatorTypeEnum oper_type, Attributes **attr,
                                                                 Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

//++ Triggers -
ex_function_get_bit_value_at::ex_function_get_bit_value_at(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

//++ MV
ex_function_is_bitwise_and_true::ex_function_is_bitwise_and_true(OperatorTypeEnum oper_type, Attributes **attr,
                                                                 Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ex_function_explode_varchar::ex_function_explode_varchar(OperatorTypeEnum oper_type, short num_operands,
                                                         Attributes **attr, Space *space, NABoolean forInsert)
    : ex_function_clause(oper_type, num_operands, attr, space),
      forInsert_(forInsert){

      };

ex_function_hash::ex_function_hash(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ex_function_hivehash::ex_function_hivehash(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ExHashComb::ExHashComb(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){};

ExHiveHashComb::ExHiveHashComb(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){};

ExHDPHash::ExHDPHash(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ExHDPHashComb::ExHDPHashComb(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ex_function_replace_null::ex_function_replace_null(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 4, attr, space){

      };

ex_function_mod::ex_function_mod(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ex_function_beginkey::ex_function_beginkey(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_endkey::ex_function_endkey(OperatorTypeEnum oper_type, Attributes **attr, Int16 bytesPerChar,
                                       NAWchar maxValue, Space *space)
    : ex_function_clause(oper_type, 2, attr, space),
      bytesPerChar_(bytesPerChar),
      maxValue_(maxValue){

      };

ex_function_mask::ex_function_mask(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ExFunctionShift::ExFunctionShift(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ex_function_bool::ex_function_bool(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

ex_function_converttimestamp::ex_function_converttimestamp(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                                           long gmtDiff)
    : ex_function_clause(oper_type, 2, attr, space),
      flags_(0),
      gmtDiff_(gmtDiff){

      };

ex_function_dateformat::ex_function_dateformat(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                               int dateformat, int num_attrs, int nCaseSensitive)
    : ex_function_clause(oper_type, num_attrs, attr, space),
      dateformat_(dateformat),
      caseSensitivity_(nCaseSensitive){

      };

ex_function_numberformat::ex_function_numberformat(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                                   int nFormatStrLen)
    : ex_function_clause(oper_type, 3, attr, space),
      formatStringLen_(nFormatStrLen){

      };

ex_function_dayofweek::ex_function_dayofweek(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_extract::ex_function_extract(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                         rec_datetime_field extractField)
    : ex_function_clause(oper_type, 2, attr, space),
      extractField_(extractField){

      };

ex_function_monthsbetween::ex_function_monthsbetween(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){

      };

ex_function_juliantimestamp::ex_function_juliantimestamp(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_exec_count::ex_function_exec_count(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space) {
  execCount_ = 0;
};

ex_function_curr_transid::ex_function_curr_transid(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

ex_function_ansi_user::ex_function_ansi_user(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

ex_function_ansi_tenant::ex_function_ansi_tenant(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){

      };

ex_function_user::ex_function_user(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_nullifzero::ex_function_nullifzero(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ex_function_nvl::ex_function_nvl(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){};

// for ngram
ex_function_firstngram::ex_function_firstngram(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ex_function_ngramcount::ex_function_ngramcount(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ex_function_queryid_extract::ex_function_queryid_extract(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 3, attr, space){};

ExFunctionUniqueId::ExFunctionUniqueId(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){};

ExFunctionRowNum::ExFunctionRowNum(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space){};

ExFunctionHbaseColumnLookup::ExFunctionHbaseColumnLookup(OperatorTypeEnum oper_type, Attributes **attr,
                                                         const char *colName, Space *space)
    : ex_function_clause(oper_type, 2, attr, space) {
  strcpy(colName_, colName);
};

ExFunctionHbaseColumnsDisplay::ExFunctionHbaseColumnsDisplay(OperatorTypeEnum oper_type, Attributes **attr, int numCols,
                                                             char *colNames, Space *space)
    : ex_function_clause(oper_type, 2, attr, space), numCols_(numCols), colNames_(colNames){};

ExFunctionHbaseColumnCreate::ExFunctionHbaseColumnCreate(OperatorTypeEnum oper_type, Attributes **attr,
                                                         short numEntries, short colNameMaxLen, int colValMaxLen,
                                                         short colValVCIndLen, Space *space)
    : ex_function_clause(oper_type, 1, attr, space),
      numEntries_(numEntries),
      colNameMaxLen_(colNameMaxLen),
      colValMaxLen_(colValMaxLen),
      colValVCIndLen_(colValVCIndLen){};

ExFunctionSequenceValue::ExFunctionSequenceValue(OperatorTypeEnum oper_type, Attributes **attr,
                                                 const SequenceGeneratorAttributes &sga, Space *space)
    : ex_function_clause(oper_type, 1, attr, space), sga_(sga), retryNum_(), flags_(0){};

ExFunctionHbaseVisibility::ExFunctionHbaseVisibility(OperatorTypeEnum oper_type, Attributes **attr, int tagType,
                                                     int colIndex, Space *space)
    : ex_function_clause(oper_type, 2, attr, space), tagType_(tagType), colIndex_(colIndex), flags_(0){};

ExFunctionHbaseVisibilitySet::ExFunctionHbaseVisibilitySet(OperatorTypeEnum oper_type, Attributes **attr,
                                                           short colIDlen, const char *colID, int visExprLen,
                                                           const char *visExpr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space), colIDlen_(colIDlen), visExprLen_(visExprLen), flags_(0) {
  memcpy(visExpr_, visExpr, visExprLen);
  visExpr_[visExprLen] = 0;

  strcpy(colID_, colID);
};

ExFunctionHbaseTimestamp::ExFunctionHbaseTimestamp(OperatorTypeEnum oper_type, Attributes **attr, int colIndex,
                                                   Space *space)
    : ex_function_clause(oper_type, 2, attr, space), colIndex_(colIndex), flags_(0){};

ExFunctionHbaseVersion::ExFunctionHbaseVersion(OperatorTypeEnum oper_type, Attributes **attr, int colIndex,
                                               Space *space)
    : ex_function_clause(oper_type, 2, attr, space), colIndex_(colIndex), flags_(0){};

ExFunctionHbaseRowid::ExFunctionHbaseRowid(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ExFunctionCastType::ExFunctionCastType(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){};

ExFunctionSVariance::ExFunctionSVariance(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 4, attr, space){};

ExFunctionSStddev::ExFunctionSStddev(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 4, attr, space){

      };

ExpRaiseErrorFunction::ExpRaiseErrorFunction(Attributes **attr, Space *space, int sqlCode, NABoolean raiseError,
                                             const char *constraintName, const char *tableName,
                                             const NABoolean hasStringExp,  // -- Triggers
                                             const char *optionalStr)
    : ex_function_clause(ITM_RAISE_ERROR, (hasStringExp ? 2 : 1), attr, space),
      theSQLCODE_(sqlCode),
      constraintName_((char *)constraintName),
      tableName_((char *)tableName) {
  setRaiseError(raiseError);

  if (optionalStr) {
    strncpy(optionalStr_, optionalStr, MAX_OPTIONAL_STR_LEN);
    optionalStr_[MAX_OPTIONAL_STR_LEN] = 0;
  } else
    optionalStr_[0] = 0;
};

ExFunctionRandomNum::ExFunctionRandomNum(OperatorTypeEnum opType, short num_operands, NABoolean simpleRandom,
                                         NABoolean useThreadIdForSeed, Attributes **attr, Space *space)
    : ex_function_clause(opType, num_operands, attr, space), flags_(0) {
  seed_ = (useThreadIdForSeed ? -1 : 0);

  if (simpleRandom) flags_ |= SIMPLE_RANDOM;
}

// MV,
ExFunctionGenericUpdateOutput::ExFunctionGenericUpdateOutput(OperatorTypeEnum oper_type, Attributes **attr,
                                                             Space *space)
    : ex_function_clause(oper_type, 1, attr, space) {}

// Triggers
ExFunctionInternalTimestamp::ExFunctionInternalTimestamp(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 1, attr, space) {}

ExFunctionSoundex::ExFunctionSoundex(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 2, attr, space){

      };

ex_function_split_part::ex_function_split_part(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_function_clause(oper_type, 4, attr, space) {}

// Triggers
ex_expr::exp_return_type ex_function_get_bit_value_at::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int buffLen = getOperand(1)->getLength(op_data[1]);

  // Get the position from operand 2.
  int pos = *(int *)op_data[2];

  // The character we look into
  int charnum = pos / 8;
  // The bit in the character we look into
  int bitnum = 8 - (pos % 8) - 1;

  // Check for error conditions.
  if ((charnum >= buffLen) || (charnum < 0)) {
    ExRaiseSqlError(heap, diagsArea, EXE_GETBIT_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  unsigned char onechar = *(unsigned char *)(op_data[1] + charnum);
  unsigned char mask = 1;
  mask = mask << bitnum;

  *((int *)op_data[0]) = (int)(mask & onechar ? 1 : 0);

  return ex_expr::EXPR_OK;
};

//++ MV
// The function returns True if any of the bits is set in both of the strings
ex_expr::exp_return_type ex_function_is_bitwise_and_true::eval(char *op_data[], CollHeap *heap,
                                                               ComDiagsArea **diagsArea) {
  int leftSize = getOperand(1)->getLength(op_data[1]);
  int rightSize = getOperand(2)->getLength(op_data[2]);

  if (leftSize != rightSize) {
    ExRaiseSqlError(heap, diagsArea, EXE_IS_BITWISE_AND_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  // Iterate through all characters until one "bitwise and" returns TRUE

  // Starting with False
  *(int *)op_data[0] = 0;
  unsigned char *leftCharPtr = (unsigned char *)(op_data[1]);
  unsigned char *rightCharPtr = (unsigned char *)(op_data[2]);

  unsigned char *endBarrier = rightCharPtr + rightSize;

  for (; rightCharPtr < endBarrier; rightCharPtr++, leftCharPtr++) {
    if ((*leftCharPtr) & (*rightCharPtr)) {
      *(int *)op_data[0] = 1;

      break;
    }
  }

  return ex_expr::EXPR_OK;
}

ExFunctionRandomSelection::ExFunctionRandomSelection(OperatorTypeEnum opType, Attributes **attr, Space *space,
                                                     float selProb)
    : ExFunctionRandomNum(opType, 1, FALSE, FALSE, attr, space) {
  if (selProb < 0) selProb = 0.0;
  selProbability_ = selProb;
  difference_ = -1;
}

ExHash2Distrib::ExHash2Distrib(Attributes **attr, Space *space)
    : ex_function_clause(ITM_HASH2_DISTRIB, 3, attr, space) {}

ExProgDistrib::ExProgDistrib(Attributes **attr, Space *space) : ex_function_clause(ITM_PROGDISTRIB, 3, attr, space) {}

ExProgDistribKey::ExProgDistribKey(Attributes **attr, Space *space)
    : ex_function_clause(ITM_PROGDISTRIBKEY, 4, attr, space) {}

ExPAGroup::ExPAGroup(Attributes **attr, Space *space) : ex_function_clause(ITM_PAGROUP, 4, attr, space) {}

ExUnPackCol::ExUnPackCol(Attributes **attr, Space *space, int width, int base, NABoolean nullsPresent)
    : width_(width), base_(base), ex_function_clause(ITM_UNPACKCOL, 3, attr, space) {
  setNullsPresent(nullsPresent);
};

ExFunctionRangeLookup::ExFunctionRangeLookup(Attributes **attr, Space *space, int numParts, int partKeyLen)
    : ex_function_clause(ITM_RANGE_LOOKUP, 3, attr, space), numParts_(numParts), partKeyLen_(partKeyLen) {}

static void setNullValue(int nStr, Attributes *tgt, char *resultNull) {
#ifdef _EMPTYSTRING_EQUIVALENT_NULL
  if (!tgt || !resultNull) return;

  if (0 == nStr) {  // if result is empty, return null
    if (tgt->getNullFlag()) {
      ExpTupleDesc::setNullValue(resultNull, tgt->getNullBitIndex(), tgt->getTupleFormat());
    }
  }
#else
  return;
#endif
}

ex_expr::exp_return_type ex_function_concat::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int len1 = 0, len2 = 0;
  if (!(getOperand(1)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 1]))
    len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  if (!(getOperand(2)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 2]))
    len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  char *resultNull = op_data[-2 * MAX_OPERANDS];
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);

    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    len2 = Attributes::trimFillerSpaces(op_data[2], prec2, len2, cs);
  }

  int max_len = getOperand(0)->getLength();
  if ((len1 + len2) > max_len) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());
    return ex_expr::EXPR_ERROR;
  }

  int actual_length = len1 + len2;

  // If operand 0 is varchar, store the sum of operand 1 length and
  // operand 2 length in the varlen area.
  getOperand(0)->setVarLength((actual_length), op_data[-MAX_OPERANDS]);

  setNullValue(actual_length, getOperand(0), op_data[-2 * MAX_OPERANDS]);
  // Now, copy the contents of operand 1 followed by the contents of
  // operand 2 into operand 0.
  str_cpy_all(op_data[0], op_data[1], len1);
  str_cpy_all(&op_data[0][len1], op_data[2], len2);

  //
  // Blankpad the target (if needed).
  //
  if ((actual_length) < max_len) {
    if (cs == CharInfo::UCS2)
      wc_str_pad((NAWchar *)&op_data[0][actual_length], (max_len - actual_length) / sizeof(NAWchar),
                 unicode_char_set::space_char());
    else
      str_pad(&op_data[0][actual_length], max_len - actual_length, ' ');
  }

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ExFunctionRepeat::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int repeatCount = *(int *)op_data[2];

  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  int resultMaxLen = getOperand(0)->getLength();

  if ((repeatCount < 0) || ((repeatCount * len1) > resultMaxLen)) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  int currPos = 0;
  for (int i = 0; i < repeatCount; i++) {
    str_cpy_all(&op_data[0][currPos], op_data[1], len1);

    currPos += len1;
  }

  // If operand 0 is varchar, store the length.
  getOperand(0)->setVarLength(currPos, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ExFunctionReplace::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(0))->getCharSet();

  // Note: all lengths are byte lengths.

  // source string
  int len1 = 0;
  if (!(getOperand(1)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 1]))
    len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  char *str1 = op_data[1];

  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(str1, prec1, len1, cs);
  }

  // if caseinsensitive search is to be done, make a copy of the source
  // string and upshift it. This string will be used to do the search.
  // The original string will be used to replace.
  char *searchStr1 = str1;
  if ((caseInsensitiveOperation()) && (heap) && (str1)) {
    searchStr1 = new (heap) char[len1];
    str_cpy_convert(searchStr1, str1, len1, 1);
  }

  // string to search for in string1
  int len2 = 0;
  if (!(getOperand(2)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 2]))
    len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  char *str2 = op_data[2];

  // string to replace string2 with in string1
  int len3 = 0;
  if (!(getOperand(3)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 3]))
    len3 = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
  char *str3 = op_data[3];

  if (cs == CharInfo::UTF8) {
    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    len2 = Attributes::trimFillerSpaces(str2, prec2, len2, cs);

    int prec3 = ((SimpleType *)getOperand(3))->getPrecision();
    len3 = Attributes::trimFillerSpaces(str3, prec3, len3, cs);
  }

  int resultMaxLen = getOperand(0)->getLength();
  char *result = op_data[0];

  char *sourceStr = searchStr1;
  char *searchStr = str2;
  int lenSourceStr = len1;     // getArgEncodedLen(0);
  int lenSearchStr = len2;     // getArgEncodedLen(1);
  int effLenSourceStr = len1;  // getArgEncodedLen(0);
  int effLenSearchStr = len2;  // getArgEncodedLen(1);

  Int16 nPasses = 1;

  if (CollationInfo::isSystemCollation(getCollation())) {
    nPasses = CollationInfo::getCollationNPasses(getCollation());
    lenSourceStr = getArgEncodedLen(0);
    lenSearchStr = getArgEncodedLen(1);

    assert(heap);

    sourceStr = new (heap) char[lenSourceStr];
    ex_function_encode::encodeCollationSearchKey((UInt8 *)str1, len1, (UInt8 *)sourceStr, lenSourceStr,
                                                 (int &)effLenSourceStr, nPasses, getCollation(), TRUE);

    searchStr = new (heap) char[lenSearchStr];

    ex_function_encode::encodeCollationSearchKey((UInt8 *)str2, len2, (UInt8 *)searchStr, lenSearchStr,
                                                 (int &)effLenSearchStr, nPasses, getCollation(), TRUE);
  }

  NABoolean done = FALSE;
  int position;
  int currPosStr1 = 0;
  int currLenStr1 = len1;
  int currPosResult = 0;
  int currLenResult = 0;
  if (0 == len2) {
    // If length of string to search for in string1 is 0, just return string1.
    str_cpy_all(&result[0], &str1[0], len1);
    currLenResult += len1;
    done = TRUE;
  }

  while (!done) {
    position = ex_function_position::findPosition(&sourceStr[currPosStr1 * nPasses], currLenStr1 * nPasses, searchStr,
                                                  effLenSearchStr, 1, nPasses, getCollation(), NULL, cs);

    if (position < 0) {
      const char *csname = CharInfo::getCharSetName(cs);
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
      *(*diagsArea) << DgString0(csname) << DgString1("REPLACE FUNCTION");
      return ex_expr::EXPR_ERROR;
    }
    if (position > 0) {
      position = position - 1;

      // copy part of str1 from currPosStr1 till position into result
      if ((currLenResult + position) > resultMaxLen) {
        if (sourceStr && sourceStr != str1) NADELETEBASIC(sourceStr, (heap));
        if (searchStr && searchStr != str2) NADELETEBASIC(searchStr, (heap));

        ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());

        return ex_expr::EXPR_ERROR;
      }

      if (position > 0) {
        str_cpy_all(&result[currPosResult], &str1[currPosStr1], position);
      }

      currPosResult += position;
      currLenResult += position;

      currPosStr1 += (position + len2);
      currLenStr1 -= (position + len2);

      // now copy str3 to result. This is the replacement.
      if ((currLenResult + len3) > resultMaxLen) {
        if (sourceStr && sourceStr != str1) NADELETEBASIC(sourceStr, (heap));
        if (searchStr && searchStr != str2) NADELETEBASIC(searchStr, (heap));

        ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());

        return ex_expr::EXPR_ERROR;
      }

      str_cpy_all(&result[currPosResult], str3, len3);
      currLenResult += len3;
      currPosResult += len3;
    } else {
      done = TRUE;

      if ((currLenResult + currLenStr1) > resultMaxLen) {
        if (sourceStr && sourceStr != str1) NADELETEBASIC(sourceStr, (heap));
        if (searchStr && searchStr != str2) NADELETEBASIC(searchStr, (heap));

        ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());

        return ex_expr::EXPR_ERROR;
      }

      if (currLenStr1 > 0) str_cpy_all(&result[currPosResult], &str1[currPosStr1], currLenStr1);
      currLenResult += currLenStr1;
    }
  }

  setNullValue(currLenResult, getOperand(0), op_data[-2 * MAX_OPERANDS]);
  // If operand 0 is varchar, store the length.
  getOperand(0)->setVarLength(currLenResult, op_data[-MAX_OPERANDS]);
  if (sourceStr && sourceStr != str1) NADELETEBASIC(sourceStr, (heap));
  if (searchStr && searchStr != str2) NADELETEBASIC(searchStr, (heap));

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ExRegexpReplace::eval(char *op_data[], CollHeap *exHeap, ComDiagsArea **diagsArea) {
  int srcLen = 0, patLen = 0, repPatLen = 0;
  if (!(getOperand(1)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 1]))
    srcLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  if (!(getOperand(2)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 2]))
    patLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  if (!(getOperand(3)->getNullFlag() && !op_data[-2 * MAX_OPERANDS + 3]))
    repPatLen = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
  int resultMaxLen = getOperand(0)->getLength();
  int replaceStrLen = resultMaxLen;
  char *result = op_data[0];

  regex_t reg;
  regmatch_t pm[10];
  const size_t nmatch = 10;
  int cflags, z;
  char ebuf[128];

  int position;
  int currPosSrc = 0;
  int currPosResult = 0;
  NABoolean raiseRegExecError = FALSE;
  NABoolean raiseOverflowError = FALSE;
  int retval;

  char *srcStr = new (exHeap) char[srcLen + 1];
  char *pattern = new (exHeap) char[patLen + 1];
  char *repPattern = new (exHeap) char[repPatLen + 1];
  char *replaceStr = new (exHeap) char[replaceStrLen + 1];

  cflags = REG_EXTENDED | REG_NEWLINE;
  srcStr[srcLen] = 0;
  pattern[patLen] = 0;
  repPattern[repPatLen] = 0;
  replaceStr[replaceStrLen] = 0;

  str_cpy_all(srcStr, op_data[1], srcLen);
  str_cpy_all(pattern, op_data[2], patLen);
  str_cpy_all(repPattern, op_data[3], repPatLen);
  str_cpy_all(replaceStr, repPattern, repPatLen);
  replaceStr[repPatLen] = 0;

  z = regcomp(&reg, pattern, cflags);

  if (z != 0) {
    // ERROR
    regerror(z, &reg, ebuf, sizeof(ebuf));
    ExRaiseSqlError(exHeap, diagsArea, (ExeErrorCode)8453);
    **diagsArea << DgString0(ebuf);
    regexpReplaceCleanup(&reg, srcStr, pattern, repPattern, replaceStr, exHeap);
    return ex_expr::EXPR_ERROR;
  }
  z = regexec(&reg, srcStr, nmatch, pm, 0);
  if (z == REG_NOMATCH) {
    // assume resultMaxLen >= srcLen
    str_cpy_all(result, srcStr, srcLen);
    currPosResult = srcLen;
  } else if (z == 0) {
    while ((z == 0) && !raiseOverflowError) {
      retval = regexpReplaceBuildReplaceStr(repPattern, repPatLen, replaceStr, replaceStrLen, srcStr, &pm[0], nmatch,
                                            diagsArea, exHeap);
      if (retval == 0) {
        regexpReplaceCleanup(&reg, srcStr, pattern, repPattern, replaceStr, exHeap);
        return ex_expr::EXPR_ERROR;
      }

      position = pm[0].rm_so;
      if (resultMaxLen >= currPosResult + position) {
        // part before match
        str_cpy_all(&result[currPosResult], &srcStr[currPosSrc], position);
        currPosResult += position;
        currPosSrc += pm[0].rm_eo;
        if (resultMaxLen >= currPosResult + replaceStrLen) {
          // replacing match pattern with replacement pattern
          str_cpy_all(&result[currPosResult], replaceStr, replaceStrLen);
          currPosResult += replaceStrLen;
          // will not read beyond srcLen as srcStr is null terminated
          z = regexec(&reg, srcStr + currPosSrc, nmatch, pm, 0);
        } else
          raiseOverflowError = TRUE;
      } else
        raiseOverflowError = TRUE;
    }  // end of while over srcStr
    if (!raiseOverflowError && (z == REG_NOMATCH)) {
      position = srcLen - currPosSrc;
      // trailing portion after all matches
      str_cpy_all(&result[currPosResult], &srcStr[currPosSrc], position);
      currPosResult += position;
      currPosSrc += position;
    } else if ((z != 0) && !raiseOverflowError)
      raiseRegExecError = TRUE;
  } else
    raiseRegExecError = TRUE;

  if (raiseRegExecError) {
    regerror(z, &reg, ebuf, sizeof(ebuf));
    ExRaiseSqlError(exHeap, diagsArea, (ExeErrorCode)8453);
    **diagsArea << DgString0(ebuf);
    regexpReplaceCleanup(&reg, srcStr, pattern, repPattern, replaceStr, exHeap);
    return ex_expr::EXPR_ERROR;
  }

  if (raiseOverflowError) {
    ExRaiseFunctionSqlError(exHeap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());
    regexpReplaceCleanup(&reg, srcStr, pattern, repPattern, replaceStr, exHeap);
    return ex_expr::EXPR_ERROR;
  }

  setNullValue(currPosResult, getOperand(0), op_data[-2 * MAX_OPERANDS]);
  // If operand 0 is varchar, store the length.
  getOperand(0)->setVarLength(currPosResult, op_data[-MAX_OPERANDS]);
  regexpReplaceCleanup(&reg, srcStr, pattern, repPattern, replaceStr, exHeap);
  return ex_expr::EXPR_OK;
}

static void regexpCleanup(regex_t *reg, char *s1, char *s2, CollHeap *heap) {
  regfree(reg);
  NADELETEBASIC(s1, heap);
  NADELETEBASIC(s2, heap);
}

ex_expr::exp_return_type ExRegexpSubstrOrCount::eval(char *op_data[], CollHeap *exHeap, ComDiagsArea **diagsArea) {
  char *result = op_data[0];
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  CharInfo::CharSet input_cs = cs;
  int srcLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  if (srcLen == 0) {
    setNullValue(0, getOperand(0), op_data[-2 * MAX_OPERANDS]);
    return ex_expr::EXPR_NULL;
  }
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    srcLen = Attributes::trimFillerSpaces(op_data[1], prec1, srcLen, cs);
  }
  Attributes *op1 = getOperand(1);
  char *srcStr = NULL;
  if (cs == CharInfo::UCS2) {  // convert input str ucs2 to utf8 since regexec can not deal with ucs2
    short intermediateLen;

    srcLen = srcLen / CharInfo::maxBytesPerChar(cs) * CharInfo::maxBytesPerChar(CharInfo::UTF8);
    srcStr = new (exHeap) char[srcLen + 1];
    if (::convDoIt(op_data[1], getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]), op1->getDatatype(),
                   op1->getPrecision(), (int)(CharInfo::UCS2), srcStr, srcLen + 1, REC_BYTE_V_ASCII, 0,
                   (int)(CharInfo::UTF8), (char *)&intermediateLen, sizeof(short), exHeap,
                   diagsArea) != ex_expr::EXPR_OK)
      return ex_expr::EXPR_ERROR;
    srcLen = intermediateLen;
    srcStr[srcLen] = 0;
    input_cs = CharInfo::UTF8;
  } else {
    srcStr = new (exHeap) char[srcLen + 1];
    srcStr[srcLen] = 0;
    str_cpy_all(srcStr, op_data[1], srcLen);
  }

  cs = ((SimpleType *)getOperand(2))->getCharSet();
  int patLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);

  if (patLen == 0) {
    setNullValue(0, getOperand(0), op_data[-2 * MAX_OPERANDS]);
    return ex_expr::EXPR_NULL;
  }
  if (cs == CharInfo::UTF8) {
    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    patLen = Attributes::trimFillerSpaces(op_data[2], prec2, patLen, cs);
  }
  Attributes *op2 = getOperand(2);
  char *pattern = NULL;
  if (cs == CharInfo::UCS2) {  // convert pattern ucs2 to utf8 since regexec can not deal with ucs2
    short intermediateLen;
    patLen = patLen / CharInfo::maxBytesPerChar(cs) * CharInfo::maxBytesPerChar(CharInfo::UTF8);
    pattern = new (exHeap) char[patLen + 1];
    if (::convDoIt(op_data[2], getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]), op2->getDatatype(),
                   op2->getPrecision(), (int)(CharInfo::UCS2), pattern, patLen + 1, REC_BYTE_V_ASCII, 0,
                   (int)(CharInfo::UTF8), (char *)&intermediateLen, sizeof(short), exHeap,
                   diagsArea) != ex_expr::EXPR_OK)
      return ex_expr::EXPR_ERROR;
    patLen = intermediateLen;
    pattern[patLen] = 0;
  } else {
    pattern = new (exHeap) char[patLen + 1];
    pattern[patLen] = 0;
    str_cpy_all(pattern, op_data[2], patLen);
  }

  regex_t reg;
  regmatch_t pm[10];
  const size_t nmatch = 10;
  char ebuf[128];
  int nStartPos = 1;
  int occurrence = 1;  // only for REGEXP_SUBSTR

  if (getNumOperands() > 3) nStartPos = *(int *)op_data[3];

  int startByteOffset = 0;
  if (nStartPos > 1) {
    startByteOffset = Attributes::convertCharToOffset(srcStr, nStartPos, srcLen, input_cs);
    if (startByteOffset < 0) {
      CharInfo::CharSet op1cs = ((SimpleType *)getOperand(1))->getCharSet();
      const char *csname = CharInfo::getCharSetName(op1cs);
      ExRaiseSqlError(exHeap, diagsArea, EXE_INVALID_CHARACTER);
      *(*diagsArea) << DgString0(csname)
                    << (bIsSubstr_ ? DgString1("REGEXP_SUBSTR FUNCTION") : DgString1("REGEXP_COUNT FUNCTION"));
      return ex_expr::EXPR_ERROR;
    }
  }

  if (bIsSubstr_ && getNumOperands() > 4) occurrence = *(int *)op_data[4];

  if (startByteOffset > srcLen) {  // If position > srcLen, return NULL
    getOperand(0)->setVarLength(0, op_data[-MAX_OPERANDS]);
    NADELETEBASIC(srcStr, exHeap);
    NADELETEBASIC(pattern, exHeap);
    return ex_expr::EXPR_NULL;
  }

  if (nStartPos <= 0) {  // position must >0
    ExRaiseSqlError(exHeap, diagsArea, EXE_OPERAND_OUT_OF_RANGE);
    *(*diagsArea) << DgInt0(nStartPos);
    NADELETEBASIC(srcStr, exHeap);
    NADELETEBASIC(pattern, exHeap);
    return ex_expr::EXPR_ERROR;
  }

  // startByteOffset start from 1
  char *pSearchStr = srcStr + startByteOffset;
  if (bIsSubstr_ && occurrence <= 0) {  // occurrence must >0
    ExRaiseSqlError(exHeap, diagsArea, EXE_OPERAND_OUT_OF_RANGE);
    *(*diagsArea) << DgInt0(occurrence);
    NADELETEBASIC(srcStr, exHeap);
    NADELETEBASIC(pattern, exHeap);
    return ex_expr::EXPR_ERROR;
  }

  int cflags = REG_EXTENDED | REG_NEWLINE;
  int z = regcomp(&reg, pattern, cflags);
  if (z != 0) {
    // ERROR
    regerror(z, &reg, ebuf, sizeof(ebuf));
    ExRaiseSqlError(exHeap, diagsArea, (ExeErrorCode)8453);
    *(*diagsArea) << DgString0(ebuf);
    regexpCleanup(&reg, srcStr, pattern, exHeap);
    return ex_expr::EXPR_ERROR;
  }

  NABoolean bRaiseError = FALSE;
  NABoolean bReturnNull = FALSE;
  if (bIsSubstr_) {  // process REGEXP_SUBSTR
    int nMatchCount = 1;
    z = regexec(&reg, pSearchStr, nmatch, pm, 0);

    if (z == REG_NOMATCH)
      // no match found, return NULL
      bReturnNull = TRUE;
    else if (z == 0) {
      while (nMatchCount != occurrence) {
        int nStep = pm[0].rm_eo ? pm[0].rm_eo : 1;
        if ((pSearchStr - srcStr + nStep) > srcLen) break;
        pSearchStr += nStep;

        z = regexec(&reg, pSearchStr, nmatch, pm, 0);
        if (z == REG_NOMATCH) {
          // no match found, return NULL
          bReturnNull = TRUE;
          break;
        } else if (z != 0) {
          // ERROR
          bRaiseError = TRUE;
          break;
        }
        // call regexec success
        ++nMatchCount;
      }
    } else
      // ERROR
      bRaiseError = TRUE;
    if (!bRaiseError && !bReturnNull) {
      cs = ((SimpleType *)getOperand(0))->getCharSet();
      int nResultLen = pm[0].rm_eo - pm[0].rm_so;
      if (cs == CharInfo::UCS2) {
        int nLen = getOperand(0)->getLength();
        char *tmpStr = new (exHeap) char[nLen + 1];
        str_cpy_all(tmpStr, pSearchStr + pm[0].rm_so, nResultLen);
        tmpStr[nLen] = 0;
        short intermediateLen;
        if (::convDoIt(tmpStr, nResultLen, REC_BYTE_F_ASCII, 0, (int)(CharInfo::UTF8), op_data[0], nLen,
                       REC_BYTE_V_ASCII, 0, (int)(CharInfo::UCS2), (char *)&intermediateLen, sizeof(short), exHeap,
                       diagsArea) != ex_expr::EXPR_OK)
          return ex_expr::EXPR_ERROR;
        nResultLen = intermediateLen;
      } else {
        str_cpy_all(result, pSearchStr + pm[0].rm_so, nResultLen);
      }
      setNullValue(nResultLen, getOperand(0), op_data[-2 * MAX_OPERANDS]);
      getOperand(0)->setVarLength(nResultLen, op_data[-MAX_OPERANDS]);
    }
  } else {
    // process REGEXP_COUNT
    int nCount = 0;

    while ((z = regexec(&reg, pSearchStr, nmatch, pm, 0)) == 0) {
      ++nCount;
      int nStep = pm[0].rm_eo ? pm[0].rm_eo : 1;
      if ((pSearchStr - srcStr + nStep) > srcLen) break;
      pSearchStr += nStep;
    }

    *(int *)op_data[0] = nCount;
  }
  if (bRaiseError) {
    regerror(z, &reg, ebuf, sizeof(ebuf));
    ExRaiseSqlError(exHeap, diagsArea, (ExeErrorCode)8453);
    *(*diagsArea) << DgString0(ebuf);
    regexpCleanup(&reg, srcStr, pattern, exHeap);
    return ex_expr::EXPR_ERROR;
  }
  if (bReturnNull) {  // set null to result
    setNullValue(0, getOperand(0), op_data[-2 * MAX_OPERANDS]);
    regexpCleanup(&reg, srcStr, pattern, exHeap);
    return ex_expr::EXPR_NULL;
  }
  regexpCleanup(&reg, srcStr, pattern, exHeap);
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_substring::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int len1_bytes = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  // Get the starting position in characters from operand 2.
  // This may be a negative value!
  int specifiedCharStartPos = *(int *)op_data[2];

  // Starting position in bytes. It can NOT be a negative value.
  int startByteOffset = 0;  // Assume beginning of buffer for now.

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  char *resultNull = op_data[-2 * MAX_OPERANDS];

  // Convert number of character to offset in buffer.
  if (specifiedCharStartPos > 1) {
    startByteOffset = Attributes::convertCharToOffset(op_data[1], specifiedCharStartPos, len1_bytes, cs);

    if (startByteOffset < 0) {
      const char *csname = CharInfo::getCharSetName(cs);
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
      *(*diagsArea) << DgString0(csname) << DgString1("SUBSTRING FUNCTION");
      return ex_expr::EXPR_ERROR;
    }
  } else { /* Leave startByteOffset at 0 */
  }

  // If operand 3 exists, get the length of substring in characters from operand
  // 3. Otherwise, if specifiedCharStartPos > 0, length is from specifiedCharStartPos char to end of buf.
  // If specifiedCharStartPos is 0, length is all of buf except last character.
  // If specifiedCharStartPos is negative, length is even less (by that negative amount).

  int inputLen_bytes = len1_bytes;           // Assume byte count = length of string for now
  int specifiedLenInChars = inputLen_bytes;  // Assume char count = byte count for now

  int prec1 = 0;

  if (getNumOperands() == 4) {
    specifiedLenInChars = *(int *)op_data[3];  // Use specified desired length for now
    if (specifiedLenInChars > len1_bytes) specifiedLenInChars = len1_bytes;
  }

  if (cs == CharInfo::UTF8) {
    prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    if (prec1) inputLen_bytes = Attributes::trimFillerSpaces(op_data[1], prec1, inputLen_bytes, cs);
  }

  // NOTE: Following formula for lastChar works even if specifiedCharStartPos is 0 or negative.

  int lastChar = specifiedLenInChars + (specifiedCharStartPos - 1);

  // The end of the substr as a byte offset
  int endOff_bytes = inputLen_bytes;  // Assume length of input for now.

  int actualLenInBytes = 0;

  if (startByteOffset >= inputLen_bytes) {
    // Nothing left in buf to copy, so endOff_bytes and actualLenInBytes are OK as is.
    startByteOffset = inputLen_bytes;  // IGNORE it if specified start > end of buffer!
    ;
  } else if (lastChar > 0) {
    endOff_bytes = Attributes::convertCharToOffset(op_data[1], lastChar + 1, inputLen_bytes, cs);

    if (endOff_bytes < 0) {
      const char *csname = CharInfo::getCharSetName(cs);
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
      *(*diagsArea) << DgString0(csname) << DgString1("SUBSTRING FUNCTION");
      return ex_expr::EXPR_ERROR;
    }
  } else
    endOff_bytes = 0;

  // Check for error conditions. endOff_bytes will be less than startByteOffset if length is
  // less than 0.
  if (endOff_bytes < startByteOffset) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_SUBSTRING_ERROR, derivedFunction(), origFunctionOperType());
    return ex_expr::EXPR_ERROR;
  }

  actualLenInBytes = endOff_bytes - startByteOffset;

  setNullValue(actualLenInBytes, getOperand(0), op_data[-2 * MAX_OPERANDS]);

  // Now, copy the substring of operand 1 from the starting position into
  // operand 0, if actualLenInBytes is greater than 0.
  if (actualLenInBytes > 0) str_cpy_all(op_data[0], &op_data[1][startByteOffset], actualLenInBytes);

  //
  // Blankpad the target (if needed).
  //
  int len0_bytes = getOperand(0)->getLength();

  if ((actualLenInBytes < len0_bytes) && prec1)
    str_pad(&op_data[0][actualLenInBytes], len0_bytes - actualLenInBytes, ' ');

  // store the length of substring in the varlen indicator.
  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(actualLenInBytes, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_trim_char::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  const int lenSrcStrSmallBuf = 128;
  char srcStrSmallBuf[lenSrcStrSmallBuf];

  const int lenTrimCharSmallBuf = 8;
  char trimCharSmallBuf[lenTrimCharSmallBuf];

  // find out the length of trim character.
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  CharInfo::CharSet cs = ((SimpleType *)getOperand(0))->getCharSet();
  char *resultNull = op_data[-2 * MAX_OPERANDS];

  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  int number_bytes = 0;

  number_bytes = Attributes::getFirstCharLength(op_data[1], len1, cs);
  if (number_bytes < 0) {
    const char *csname = CharInfo::getCharSetName(cs);
    ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
    *(*diagsArea) << DgString0(csname) << DgString1("TRIM FUNCTION");
    return ex_expr::EXPR_ERROR;
  }

  // len1 (length of trim character) must be 1 character. Raise an exception if greater
  // than 1.
  // rtrim now support more than 1 trim character
  if ((len1 != number_bytes) && (getTrimMode() != 0)) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_TRIM_ERROR, derivedFunction(), origFunctionOperType());
    return ex_expr::EXPR_ERROR;
  }

  int len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);

  if (cs == CharInfo::UTF8)  // If so, must ignore any filler spaces at end of string
  {
    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    len2 = Attributes::trimFillerSpaces(op_data[2], prec2, len2, cs);
  }

  Int16 nPasses = 1;
  char *trimChar = op_data[1];
  char *srcStr = op_data[2];
  int lenSrcStr = len2;
  int lenTrimChar = len1;
  int effLenSourceStr = len2;
  int effLenTrimChar = len1;

  // case of collation --
  if (CollationInfo::isSystemCollation(getCollation())) {
    nPasses = CollationInfo::getCollationNPasses(getCollation());

    // get the length of the encoded source string
    lenSrcStr = getSrcStrEncodedLength();

    // get length of encoded trim character
    lenTrimChar = getTrimCharEncodedLength();

    assert(heap);

    if (lenSrcStr <= lenSrcStrSmallBuf) {
      srcStr = srcStrSmallBuf;
    } else {
      srcStr = new (heap) char[lenSrcStr];
    }

    // get encoded key
    ex_function_encode::encodeCollationSearchKey((UInt8 *)op_data[2], len2, (UInt8 *)srcStr, lenSrcStr,
                                                 (int &)effLenSourceStr, nPasses, getCollation(), FALSE);

    if (lenTrimChar <= lenTrimCharSmallBuf) {
      trimChar = trimCharSmallBuf;
    } else {
      trimChar = new (heap) char[lenTrimChar];
    }

    // get encoded key
    ex_function_encode::encodeCollationSearchKey((UInt8 *)op_data[1], len1, (UInt8 *)trimChar, lenTrimChar,
                                                 (int &)effLenTrimChar, nPasses, getCollation(), FALSE);
  }
  // Find how many leading characters in operand 2 correspond to the trim
  // character.
  int len0 = len2;
  int start = 0;
  NABoolean notEqualFlag = 0;

  if ((getTrimMode() == 1) || (getTrimMode() == 2)) {
    while (start <= len2 - len1) {
      for (int i = 0; i < lenTrimChar; i++) {
        if (trimChar[i] != srcStr[start * nPasses + i]) {
          notEqualFlag = 1;
          break;
        }
      }
      if (notEqualFlag == 0) {
        start += len1;
        len0 -= len1;
      } else
        break;
    }
  }

  // Find how many trailing characters in operand 2 correspond to the trim
  // character.
  int end = len2;
  int endt;
  int numberOfCharacterInBuf;
  int bufferLength = end - start;
  const int smallBufSize = 128;
  char smallBuf[smallBufSize];

  notEqualFlag = 0;

  if ((getTrimMode() == 0) || (getTrimMode() == 2)) {
    char *charLengthInBuf;

    if (bufferLength <= smallBufSize)
      charLengthInBuf = smallBuf;
    else
      charLengthInBuf = new (heap) char[bufferLength];

    numberOfCharacterInBuf = Attributes::getCharLengthInBuf(op_data[2] + start, op_data[2] + end, charLengthInBuf, cs);

    if (numberOfCharacterInBuf < 0) {
      if (srcStr && srcStr != op_data[2]) NADELETEBASIC(srcStr, (heap));
      if (trimChar && trimChar != op_data[1]) NADELETEBASIC(trimChar, (heap));

      const char *csname = CharInfo::getCharSetName(cs);
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
      *(*diagsArea) << DgString0(csname) << DgString1("TRIM FUNCTION");
      return ex_expr::EXPR_ERROR;
    }

    while (end > start) {
      int currentLenInSrc = charLengthInBuf[--numberOfCharacterInBuf];
      endt = end - currentLenInSrc;

      int position = ex_function_position::findPosition(trimChar, lenTrimChar, srcStr + endt * nPasses, currentLenInSrc,
                                                        1, nPasses, getCollation(), NULL, cs);

      if (position > 0) {
        end = endt;
        len0 -= currentLenInSrc;
      } else
        break;
    }
    if (bufferLength > smallBufSize) NADELETEBASIC(charLengthInBuf, heap);
  }

  // Result is always a varchar.
  // store the length of trimmed string in the varlen indicator.
  getOperand(0)->setVarLength(len0, op_data[-MAX_OPERANDS]);

  setNullValue(len0, getOperand(0), op_data[-2 * MAX_OPERANDS]);
  // Now, copy operand 2 skipping the trim characters into
  // operand 0.

  if (len0 > 0) str_cpy_all(op_data[0], &op_data[2][start], len0);

  if (srcStr && srcStr != srcStrSmallBuf && srcStr != op_data[2]) NADELETEBASIC(srcStr, (heap));
  if (trimChar && trimChar != trimCharSmallBuf && trimChar != op_data[1]) NADELETEBASIC(trimChar, (heap));

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ex_function_lower::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  CharInfo::CharSet cs = ((SimpleType *)getOperand(0))->getCharSet();

  // for mantis-19283
  // LOWER(col) = 'MALE', col's type is CHAR(8) and character set is UTF8,
  // in the case, len1=32. When comparing later, the left operand's length use 32,
  // so we can't trim the spaces.
  if (cs == CharInfo::UTF8 && getOperand(1)->getVCIndicatorLength() > 0) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  getOperand(0)->setVarLength(len1, op_data[-MAX_OPERANDS]);

  cnv_charset charset = convertCharsetEnum(cs);

  int number_bytes;
  int total_bytes_out = 0;
  char tmpBuf[4];

  UInt32 UCS4value;
  UInt16 UCS2value;

  // Now, copy the contents of operand 1 after the case change into operand 0.
  int len0 = 0;
  if (cs == CharInfo::ISO88591) {
    while (len0 < len1) {
      op_data[0][len0] = TOLOWER(op_data[1][len0]);
      ++len0;
      ++total_bytes_out;
    }
  } else {
    // If character set is UTF8 or SJIS or ?, convert the string to UCS2,
    // call UCS2 lower function and convert the string back.
    while (len0 < len1) {
      number_bytes = LocaleCharToUCS4(op_data[1] + len0, len1 - len0, &UCS4value, charset);

      if (number_bytes < 0) {
        const char *csname = CharInfo::getCharSetName(cs);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
        *(*diagsArea) << DgString0(csname) << DgString1("LOWER FUNCTION");
        return ex_expr::EXPR_ERROR;
      }

      if (number_bytes == 1 && (op_data[1][len0] & 0x80) == 0) {
        op_data[0][len0] = TOLOWER(op_data[1][len0]);
        ++len0;
        ++total_bytes_out;
      } else {
        UCS2value = UCS4value & 0XFFFF;

        UCS4value = unicode_char_set::to_lower(*(NAWchar *)&UCS2value);

        int number_bytes_out =
            UCS4ToLocaleChar((const UInt32 *)&UCS4value, tmpBuf, CharInfo::maxBytesPerChar(cs), charset);

        if (number_bytes_out < 0) {
          const char *csname = CharInfo::getCharSetName(cs);
          ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
          *(*diagsArea) << DgString0(csname) << DgString1("LOWER FUNCTION");
          return ex_expr::EXPR_ERROR;
        }

        for (int j = 0; j < number_bytes_out; j++) {
          op_data[0][total_bytes_out] = tmpBuf[j];
          total_bytes_out++;
        }
        len0 += number_bytes;
      }
    }
  }
  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(total_bytes_out, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ex_function_upper::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  int len0 = getOperand(0)->getLength();

  int in_pos = 0;
  int out_pos = 0;

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  int number_bytes;

  UInt32 UCS4value = 0;
  UInt16 UCS2value = 0;

  // Now, copy the contents of operand 1 after the case change into operand 0.
  if (cs == CharInfo::ISO88591) {
    while (in_pos < len1) {
      op_data[0][out_pos] = TOUPPER(op_data[1][in_pos]);
      ++in_pos;
      ++out_pos;
    }
  } else {
    cnv_charset charset = convertCharsetEnum(cs);

    // If character set is UTF8 or SJIS or ?, convert the string to UCS2,
    // call UCS2 upper function and convert the string back.
    while (in_pos < len1) {
      number_bytes = LocaleCharToUCS4(op_data[1] + in_pos, len1 - in_pos, &UCS4value, charset);

      if (number_bytes < 0) {
        const char *csname = CharInfo::getCharSetName(cs);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
        *(*diagsArea) << DgString0(csname) << DgString1("UPPER FUNCTION");
        return ex_expr::EXPR_ERROR;
      }

      if (number_bytes == 1 && (op_data[1][in_pos] & 0x80) == 0) {
        op_data[0][out_pos] = TOUPPER(op_data[1][in_pos]);
        ++in_pos;
        ++out_pos;
      } else {
        in_pos += number_bytes;

        UCS2value = UCS4value & 0XFFFF;
        NAWchar wcUpshift[3];
        int charCnt = 1;  // Default count to 1

        // search against unicode_lower2upper_mapping_table_full
        NAWchar *tmpWCP = unicode_char_set::to_upper_full(UCS2value);
        if (tmpWCP) {
          wcUpshift[0] = *tmpWCP++;
          wcUpshift[1] = *tmpWCP++;
          wcUpshift[2] = *tmpWCP;
          charCnt = (*tmpWCP) ? 3 : 2;
        } else
          wcUpshift[0] = unicode_char_set::to_upper(UCS2value);

        for (int ii = 0; ii < charCnt; ii++) {
          UInt32 UCS4_val = wcUpshift[ii];
          char tmpBuf[8];

          int out_bytes = UCS4ToLocaleChar((const UInt32 *)&UCS4_val, tmpBuf, CharInfo::maxBytesPerChar(cs), charset);
          if (out_bytes < 0) {
            const char *csname = CharInfo::getCharSetName(cs);
            ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
            *(*diagsArea) << DgString0(csname) << DgString1("UPPER FUNCTION");
            return ex_expr::EXPR_ERROR;
          }
          if (out_pos + out_bytes > len0) {
            ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());
            return ex_expr::EXPR_ERROR;
          }
          for (int j = 0; j < out_bytes; j++) {
            op_data[0][out_pos] = tmpBuf[j];
            ++out_pos;
          }
        }
      }
    }
  }
  getOperand(0)->setVarLength(out_pos, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_oct_length::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  // Move operand's length into result.
  // The data type of result is long.
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }
  *(int *)op_data[0] = len1;

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ExFunctionAscii::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  if (len1 > 0) {
    switch (getOperType()) {
      case ITM_UNICODE_CODE_VALUE: {
        UInt16 temp;
        str_cpy_all((char *)&temp, op_data[1], 2);
        *(int *)op_data[0] = temp;
      } break;

      case ITM_NCHAR_MP_CODE_VALUE: {
        UInt16 temp;
#if defined(NA_LITTLE_ENDIAN)
        // swap the byte order on little-endian machines as NCHAR_MP charsets are stored
        // in multi-byte form (i.e. in big-endian order).
        temp = reversebytesUS(*((NAWchar *)op_data[1]));
#else
        str_cpy_all((char *)&temp, op_data[1], 2);
#endif
        *(UInt32 *)op_data[0] = temp;
      } break;

      case ITM_ASCII: {
        int val = (unsigned char)(op_data[1][0]);

        if ((val > 0x7F) && (cs != CharInfo::ISO88591)) {
          ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
          **diagsArea << DgString0("ASCII");

          if (derivedFunction()) {
            **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
            **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
          }

          return ex_expr::EXPR_ERROR;
        }
        *(UInt32 *)op_data[0] = (unsigned char)(op_data[1][0]);
        break;
      }
      case ITM_CODE_VALUE:
      default: {
        UInt32 UCS4value = 0;
        if (cs == CharInfo::ISO88591)
          UCS4value = *(unsigned char *)(op_data[1]);
        else {
          UInt32 firstCharLen = LocaleCharToUCS4(op_data[1], len1, &UCS4value, convertCharsetEnum(cs));

          if (firstCharLen < 0) {
            const char *csname = CharInfo::getCharSetName(cs);
            ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
            *(*diagsArea) << DgString0(csname) << DgString1("CODE_VALUE FUNCTION");
            return ex_expr::EXPR_ERROR;
          }
        }
        *(int *)op_data[0] = UCS4value;
        break;
      }
    }
  } else
    *(int *)op_data[0] = 0;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionMaxBytes::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  if (len1 > 0) {
    switch (getOperType()) {
      case ITM_UNICODE_MAXBYTES:
      case ITM_NCHAR_MP_MAXBYTES: {
        *(int *)op_data[0] = 2;
        break;
      }

      case ITM_UTF8_MAXBYTES: {
        *(int *)op_data[0] = maxBytesInUTF8String(op_data[1], len1);
        break;
      }

      case ITM_MAXBYTES:
      default: {
        *(int *)op_data[0] = 1;
        break;
      }
    }
  } else
    *(int *)op_data[0] = 0;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionChar::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  UInt32 asciiCode = *(int *)op_data[1];
  int charLength = 1;

  CharInfo::CharSet cs = ((SimpleType *)getOperand(0))->getCharSet();

  if (getOperType() == ITM_CHAR) {
    if (cs == CharInfo::ISO88591) {
      if (asciiCode < 0 || asciiCode > 0xFF) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        **diagsArea << DgString0("CHAR");
        if (derivedFunction()) {
          **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
          **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
        }
        return ex_expr::EXPR_ERROR;
      } else {
        op_data[0][0] = (char)asciiCode;
        getOperand(0)->setVarLength(1, op_data[-MAX_OPERANDS]);
        return ex_expr::EXPR_OK;
      }
    } else  // Must be UTF8 (at least until we support SJIS or some other multi-byte charset)
    {
      int len0_bytes = getOperand(0)->getLength();

      int *UCS4ptr = (int *)op_data[1];

      int charLength = UCS4ToLocaleChar(UCS4ptr, (char *)op_data[0], len0_bytes, cnv_UTF8);

      if (charLength < 0) {
        const char *csname = CharInfo::getCharSetName(cs);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
        *(*diagsArea) << DgString0(csname) << DgString1("CHAR FUNCTION");

        if (derivedFunction()) {
          **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
          **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
        }
        return ex_expr::EXPR_ERROR;
      } else {
        if (charLength < len0_bytes) str_pad(((char *)op_data[0]) + charLength, len0_bytes - charLength, ' ');
        getOperand(0)->setVarLength(charLength, op_data[-MAX_OPERANDS]);
      }
    }
  } else {
    // ITM_UNICODE_CHAR or ITM_NCHAR_MP_CHAR

    // check if the code value is legal for UNICODE only. No need
    // for KANJI/KSC5601 as both take code-point values with any bit-patterns.
    if ((getOperType() == ITM_UNICODE_CHAR) && (asciiCode < 0 || asciiCode >= 0xFFFE)) {
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      **diagsArea << DgString0("CHAR");

      if (derivedFunction()) {
        **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
        **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
      }

      return ex_expr::EXPR_ERROR;
    }

    NAWchar wcharCode = (NAWchar)asciiCode;

#if defined(NA_LITTLE_ENDIAN)
    // swap the byte order on little-endian machines as NCHAR_MP charsets are stored
    // in multi-byte form (i.e. in big-endian order).
    if (getOperType() == ITM_NCHAR_MP_CHAR) {
      *(NAWchar *)op_data[0] = reversebytesUS(wcharCode);
    } else
      *(NAWchar *)op_data[0] = wcharCode;
#else
    *(NAWchar *)op_data[0] = wcharCode;
#endif
  }
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_char_length::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int offset = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  int numOfChar = 0;

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  if (CharInfo::maxBytesPerChar(cs) == 1) {
    *(int *)op_data[0] = offset;
    return ex_expr::EXPR_OK;
  }

  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    offset = Attributes::trimFillerSpaces(op_data[1], prec1, offset, cs);
  }

  // convert to number of character
  numOfChar = Attributes::convertOffsetToChar(op_data[1], offset, cs);

  if (numOfChar < 0) {
    const char *csname = CharInfo::getCharSetName(cs);
    ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
    *(*diagsArea) << DgString0(csname) << DgString1("CHAR FUNCTION");
    return ex_expr::EXPR_ERROR;
  }

  // Move operand's length into result.
  // The data type of result is long.

  *(int *)op_data[0] = numOfChar;

  return ex_expr::EXPR_OK;
};

NABoolean ex_function_clause::swapBytes(int datatype, const char *srcData, char *tgtData) {
  NABoolean dataWasSwapped = FALSE;
  switch (datatype) {
    case REC_BIN16_SIGNED:
    case REC_BIN16_UNSIGNED:
    case REC_BPINT_UNSIGNED:
      *(unsigned short *)tgtData = bswap_16(*(unsigned short *)srcData);
      dataWasSwapped = TRUE;
      break;

    case REC_BIN32_SIGNED:
    case REC_BIN32_UNSIGNED:
      *(int *)tgtData = bswap_32(*(int *)srcData);
      dataWasSwapped = TRUE;
      break;

    case REC_BIN64_SIGNED:
      *(long *)tgtData = bswap_64(*(long *)srcData);
      dataWasSwapped = TRUE;
      break;
  }  // switch

  return dataWasSwapped;
}

ex_expr::exp_return_type ExFunctionConvertHex::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  static const char HexArray[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  if (getOperType() == ITM_CONVERTTOHEX) {
    int i;
    if (DFS2REC::isDoubleCharacter(getOperand(1)->getDatatype())) {
      NAWchar *w_p = (NAWchar *)op_data[1];
      int w_len = len1 / sizeof(NAWchar);
      for (i = 0; i < w_len; i++) {
        op_data[0][4 * i] = HexArray[0x0F & w_p[i] >> 12];
        op_data[0][4 * i + 1] = HexArray[0x0F & w_p[i] >> 8];
        op_data[0][4 * i + 2] = HexArray[0x0F & w_p[i] >> 4];
        op_data[0][4 * i + 3] = HexArray[0x0F & w_p[i]];
      }
    } else {
      CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
      if (cs == CharInfo::UTF8) {
        int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
        len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
      }

      char *srcData = op_data[1];
#if defined(NA_LITTLE_ENDIAN)
      long temp;
      if (swapBytes(getOperand(1)->getDatatype(), srcData, (char *)&temp)) srcData = (char *)&temp;
#endif

      for (i = 0; i < len1; i++) {
        op_data[0][2 * i] = HexArray[0x0F & srcData[i] >> 4];
        op_data[0][2 * i + 1] = HexArray[0x0F & srcData[i]];
      }
    }

    getOperand(0)->setVarLength(2 * len1, op_data[-MAX_OPERANDS]);
  } else {
    // convert from hex.

    // make sure that length is an even number.
    if ((len1 % 2) != 0) {
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      **diagsArea << DgString0("CONVERTFROMHEX");
      if (derivedFunction()) {
        **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
        **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
      }
      return ex_expr::EXPR_ERROR;
    }

    int i = 0;
    int j = 0;
    while (i < len1) {
      if (((op_data[1][i] >= '0') && (op_data[1][i] <= '9')) ||
          ((op_data[1][i] >= 'A') && (op_data[1][i] <= 'F')) &&
              (((op_data[1][i + 1] >= '0') && (op_data[1][i + 1] <= '9')) ||
               ((op_data[1][i + 1] >= 'A') && (op_data[1][i + 1] <= 'F')))) {
        unsigned char upper4Bits;
        unsigned char lower4Bits;
        if ((op_data[1][i] >= '0') && (op_data[1][i] <= '9'))
          upper4Bits = (unsigned char)(op_data[1][i]) - '0';
        else
          upper4Bits = (unsigned char)(op_data[1][i]) - 'A' + 10;

        if ((op_data[1][i + 1] >= '0') && (op_data[1][i + 1] <= '9'))
          lower4Bits = (unsigned char)(op_data[1][i + 1]) - '0';
        else
          lower4Bits = (unsigned char)(op_data[1][i + 1]) - 'A' + 10;

        op_data[0][j] = (upper4Bits << 4) | lower4Bits;

        i += 2;
        j++;
      } else {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        **diagsArea << DgString0("CONVERTFROMHEX");

        if (derivedFunction()) {
          **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
          **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
        }

        return ex_expr::EXPR_ERROR;
      }
    }  // while

    getOperand(0)->setVarLength(len1 / 2, op_data[-MAX_OPERANDS]);
  }  // CONVERTFROMHEX

  return ex_expr::EXPR_OK;
}

int ex_function_position::errorChecks(int startPos, int occurrence, NABoolean isInstr, const char *userTextStr,
                                      CharInfo::Collation collation, CharInfo::CharSet cs, CollHeap *heap,
                                      ComDiagsArea **diagsArea) {
  // startPos is 1-based.
  // If POSITION/LOCATE, startPos must be > 0.
  // If INSTR, startPos can be any value.
  // -ve means do reverse lookup.
  if (NOT isInstr) {
    if (startPos == 0) {
      ExRaiseSqlError(heap, diagsArea, -1571);
      *(*diagsArea) << DgString0("START POSITION") << DgString1(userTextStr);
      return -1;
    }

    if (startPos < 0) {
      ExRaiseSqlError(heap, diagsArea, -1572);
      *(*diagsArea) << DgString0("START POSITION") << DgString1(userTextStr);
    }
  } else {
    if ((startPos < 0) && (cs == CharInfo::UTF8)) {
      NAString errStr(userTextStr);
      errStr += " on UTF8 characters";
      ExRaiseSqlError(heap, diagsArea, -1572);
      *(*diagsArea) << DgString0("START POSITION") << DgString1(errStr);

      return -1;
    }
  }

  // reverse scan is only supported for default collation.
  // Note: we dont support other collations, like czech, so it is
  // unclear when/if this code will beexecuted.
  // Return error if this is system collation.
  if ((startPos < 0) && (CollationInfo::isSystemCollation(collation))) {
    NAString errStr(" datatype with system collation");
    ExRaiseSqlError(heap, diagsArea, -1572);
    *(*diagsArea) << DgString0("START POSITION") << DgString1(errStr);

    return -1;
  }

  if (occurrence < 0) {
    ExRaiseSqlError(heap, diagsArea, -1572);
    *(*diagsArea) << DgString0("OCCURRENCE") << DgString1(userTextStr);

    return -1;
  }

  if (occurrence == 0) {
    ExRaiseSqlError(heap, diagsArea, -1571);
    *(*diagsArea) << DgString0("OCCURRENCE") << DgString1(userTextStr);

    return -1;
  }

  return 0;
}

// this method is called from pcode to evaluate position function when
// iso charset is used and start position or number of occurrence are
// not specified.
int ex_function_position::findPosition(char *pat, int patLen, char *src, int srcLen, NABoolean patternInFront) {
  int i, j, k;

  // Pattern must be able to "fit" in source string
  if (patLen > srcLen) return 0;

  // One time check at beginning of src string if flag indicate so.
  if (patternInFront) return ((str_cmp(pat, src, patLen) == 0) ? 1 : 0);

  // Search for pattern throughout the src string
  for (i = 0; (i + patLen) <= srcLen; i++) {
    NABoolean found = TRUE;
    for (j = i, k = 0; found && (k < patLen); k++, j++) {
      if (src[j] != pat[k]) found = 0;
    }

    if (found) return i + 1;
  }

  return 0;
}

int ex_function_position::findPosition(char *sourceStr, int sourceLen, char *searchStr, int searchLen,
                                       short bytesPerChar, Int16 nPasses, CharInfo::Collation collation,
                                       int *numChars,  // returns number of characters, if passed in
                                       CharInfo::CharSet cs) {
  // If searchLen is <= 0 or searchLen > sourceLen or
  // if searchStr is not present in sourceStr,
  // return a position of 0;
  // otherwise return the position of searchStr in
  // sourceStr.

  if (numChars) *numChars = 0;

  if (searchLen <= 0) return 0;

  int position = 1;
  int collPosition = 1;
  int char_count = 1;
  while (position + searchLen - 1 <= sourceLen) {
    short rc;
    rc = str_cmp(searchStr, &sourceStr[position - 1], (int)searchLen);
    if (rc != 0)  // did not find a match
    {
      if (CollationInfo::isSystemCollation(collation)) {
        position += nPasses;
        collPosition++;
      } else {
        int number_bytes = Attributes::getFirstCharLength(&sourceStr[position - 1], sourceLen - position + 1, cs);

        if (number_bytes <= 0) return (int)-1;

        ++char_count;
        if (cs == CharInfo::UCS2)
          position += number_bytes * unicode_char_set::bytesPerChar();
        else
          position += number_bytes;
      }
    } else  // found a match
    {
      if (CollationInfo::isSystemCollation(collation)) {
        return collPosition;
      } else {
        if (numChars) *numChars = char_count;

        return position;
      }
    }  // else
  }    // while

  return 0;
}

int ex_function_position::findPositionInReverse(char *searchStr, int searchLen, char *sourceStr, int sourceLen,
                                                int occurrence, CharInfo::CharSet cs) {
  // If searchLen is <= 0 or searchLen > sourceLen or
  // if searchStr is not present in sourceStr,
  // return a position of 0;
  // otherwise return the position of searchStr in
  // sourceStr.

  if (searchLen < 0) return 0;

  // position is 0-based
  int position = sourceLen - searchLen;

  int occ = 1;
  while (position >= 0) {
    if (str_cmp(searchStr, &sourceStr[position], (int)searchLen) != 0) {
      // not found.
      int number_bytes = (cs == CharInfo::UCS2 ? unicode_char_set::bytesPerChar()
                                               : Attributes::getFirstCharLength(&sourceStr[position], searchLen, cs));

      if (number_bytes <= 0) return (int)-1;

      position -= number_bytes;
    } else  // found matching string
    {
      if (occ == occurrence) {
        int numOfChars = Attributes::convertOffsetToChar(sourceStr, position, cs);
        return numOfChars + 1;
      }

      occ++;
      position -= searchLen;
    }
  }  // while

  return 0;
}

// This method is used for all charsets (iso, ucs2, utf8).
//
// There is some existing code that deals with collations but it is not
// clear if that will be executed since we dont support non-default/system
// collation.
//
ex_expr::exp_return_type ex_function_position::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  NABoolean isInstr = getIsInstr();

  NAString userTextStr(getUserTextStr() ? getUserTextStr() : "POSITION");
  userTextStr += " function";

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  short bpc = (cs == CharInfo::UCS2 ? unicode_char_set::bytesPerChar() : 1);

  // null processing need to be done in eval
  if (!isNullRelevant()) {
    // if any child is null, return null
    ex_expr::exp_return_type rc = ex_clause::processNulls(&op_data[-2 * MAX_OPERANDS], heap, diagsArea);
    if (rc != ex_expr::EXPR_OK) return rc;
  }

  // result position is 1 based. First character position is 1.

  // len1 and len2 are byte lengths.

  // search for operand 1 (pattern to be searched)
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  // in operand 2 (source string)
  int len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  if (cs == CharInfo::UTF8) {
    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    len2 = Attributes::trimFillerSpaces(op_data[2], prec2, len2, cs);
  }

  // Default value is 1, if startCharOffset and occurrence are not specified
  int startCharOffset = 1;
  int occurrence = 1;

  // If startCharOffset is not specified, startBytePos is 1.
  // Computed startBytePos is 1 based.
  int startBytePos = 1;

  if (getNumOperands() >= 4)  // start position and optional occurrence specified
  {
    // operand #3
    startCharOffset = *(int *)op_data[3];
    if (getNumOperands() == 5)
      // operand #4
      occurrence = *(int *)op_data[4];

    if (errorChecks(startCharOffset, occurrence, isInstr, userTextStr.data(), getCollation(), cs, heap, diagsArea))
      return ex_expr::EXPR_ERROR;

    if (cs == CharInfo::UCS2) {
      // input startCharOffset is number of characters.
      // convert it to byte offset.
      if (startCharOffset > 0)
        startBytePos = startCharOffset * bpc - 1;
      else
        startBytePos = len2 - (ABS(startCharOffset) * bpc) + 1;
    } else if (cs == CharInfo::UTF8) {
      // startCharOffset cannot be -ve. That has already been validated.
      startBytePos = Attributes::convertCharToOffset(op_data[2], startCharOffset, len2, cs);
      if (startBytePos < 0) {
        const char *csname = CharInfo::getCharSetName(cs);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
        *(*diagsArea) << DgString0(csname) << DgString1(userTextStr);
        return ex_expr::EXPR_ERROR;
      }

      startBytePos++;
    } else  // ISO
    {
      if (startCharOffset > 0)
        startBytePos = startCharOffset;
      else
        startBytePos = len2 - ABS(startCharOffset) + 1;
    }
  }  // start position and (optional) occurrence specified

  // if this was INSTR function, and string to search with is zero length,
  // return NULL
  if (isInstr) {
    if (len1 == 0) {
      // return NULL as result
      Attributes *tgt = getOperand(0);
      char *tgtNull = op_data[-2 * MAX_OPERANDS];

      // move null value to result
      ExpTupleDesc::setNullValue(tgtNull, tgt->getNullBitIndex(), tgt->getTupleFormat());

      return ex_expr::EXPR_NULL;
    }

    if (startCharOffset == 0)  // return 0 for INSTR if specified startpos is 0
    {
      // Now copy the position into result which is a long.
      *(int *)op_data[0] = 0;

      return ex_expr::EXPR_OK;
    }
  }  // isInstr

  int position = 0;
  int numChars = 0;
  int tNumChars = 0;
  if (len1 > len2)  // pattern longer than source
  {
    position = 0;
    numChars = 0;
  } else if (len1 <= 0) {
    position = 1;
    numChars = 1;
  }
  // search in reverse if -ve startPos is specified.
  else if (startCharOffset < 0) {
    // op_data[1](searchStr) is the string to be searched for. (len1)
    // op_data[2](sourceStr) is the string to be searched in.  (len2)

    len2 = startBytePos + len1 - 1;

    position = findPositionInReverse(op_data[1], len1, op_data[2], len2, occurrence, cs);

    // convert to number of chars if ucs2 is used.
    // Note: reverse search is not supported for utf8
    if (position > 0) position = (position - 1) / bpc + 1;

    numChars = position;
  }     // reverse
  else  // forward scan
  {
    // operand2/srcStr is the string to be searched in.
    // startBytePos is 1-based.
    char *srcStr = &op_data[2][startBytePos - 1];
    len2 -= (startBytePos - 1);

    short nPasses = CollationInfo::getCollationNPasses(getCollation());
    for (int occ = 1; occ <= occurrence; occ++) {
      tNumChars = 0;
      position = findPosition(srcStr, len2, op_data[1], len1, 1, nPasses, getCollation(), &tNumChars, cs);
      if (position > 0) numChars += tNumChars;

      if (position < 0)  // error condition
        break;

      if ((occ < occurrence) && (position > 0))  // found a matching string
      {
        // skip the current matched string and continue
        srcStr += (position + len1 - 1);
        len2 -= (position + len1 - 1);
        startBytePos += (position + len1 - 1);
      }
    }  // for occ

    if (position > 0)  // found matching string
    {
      position += (startBytePos - 1);
      position = (position - 1) / bpc + 1;

      if (cs == CharInfo::UTF8)
        numChars += (startCharOffset - 1);
      else
        numChars = position;
    } else if (position == 0)
      numChars = 0;
  }  // forward scan

  if (position < 0) {
    const char *csname = CharInfo::getCharSetName(cs);
    ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
    *(*diagsArea) << DgString0(csname) << DgString1(userTextStr);
    return ex_expr::EXPR_ERROR;
  }

  // Now copy the position into result which is a long.
  *(int *)op_data[0] = numChars;

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ex_function_char_length_doublebyte::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  // Move operand's length into result.
  // The data type of result is long.

  *(int *)op_data[0] = (getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1])) >> 1;

  return ex_expr::EXPR_OK;
};

static int findTokenPosition(char *sourceStr, int sourceLen, char *searchStr, int searchLen, short bytesPerChar) {
  // If searchLen is <= 0 or searchLen > sourceLen or
  // if searchStr is not present in sourceStr,
  // return a position of 0;
  // otherwise return the position of searchStr in
  // sourceStr.
  int position = 0;
  if (searchLen <= 0)
    position = 0;
  else {
    NABoolean found = FALSE;
    position = 1;
    while (position + searchLen - 1 <= sourceLen && !found) {
      if (str_cmp(searchStr, &sourceStr[position - 1], (int)searchLen) != 0)
        position += bytesPerChar;
      else
        found = 1;
    }
    if (!found) position = 0;
  }

  return position;
}

ex_expr::exp_return_type ExFunctionTokenStr::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  // search for operand 1
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  // in operand 2
  int len2 = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  if (cs == CharInfo::UTF8) {
    int prec2 = ((SimpleType *)getOperand(2))->getPrecision();
    len2 = Attributes::trimFillerSpaces(op_data[2], prec2, len2, cs);
  }

  int position;
  position = findTokenPosition(op_data[2], len2, op_data[1], len1, 1);
  if (position <= 0) {
    std::string temp(op_data[1], len1);
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_TOKEN_NOT_FOUND);
    *(*diagsArea) << DgString0(temp.c_str());

    return ex_expr::EXPR_ERROR;
  }

  int startPos = position + len1 - 1;

  int endPos = -1;
  if (getNumOperands() == 4) {
    int len3 = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
    if (cs == CharInfo::UTF8) {
      int prec3 = ((SimpleType *)getOperand(3))->getPrecision();
      len3 = Attributes::trimFillerSpaces(op_data[3], prec3, len3, cs);
    }

    // search for end token op_data[3]
    position = findTokenPosition(op_data[2], len2, op_data[3], len3, 1);
    if (position > 0)  // found it
    {
      endPos = position - 1 - 1;
    } else {
      // not found. Return remaining string starting at startPos
      endPos = len2;
    }

    /*
    // or should an error be returned if end token not found.
    // Maybe based on an option to tokekstr function.
    // TBD.
    if (position <= 0) // not found
    {
    std::string temp(op_data[3], len3);

    ExRaiseFunctionSqlError(heap, diagsArea, EXE_TOKEN_NOT_FOUND);
    *(*diagsArea) << DgString0(temp.c_str());

    return ex_expr::EXPR_ERROR;
    }

    endPos = position -1 -1;
    */
  } else {
    if (op_data[2][startPos] == '\'') {
      // find the ending single quote.
      startPos++;
      endPos = startPos;
      while ((endPos < len2) && (op_data[2][endPos] != '\'')) endPos++;
    } else {
      // find the ending space character
      endPos = startPos;
      while ((endPos < len2) && (op_data[2][endPos] != ' ')) endPos++;
    }
  }

  if ((endPos - startPos) <= 0) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  str_cpy_all(op_data[0], &op_data[2][startPos], (endPos - startPos));

  // If result is a varchar, store the length of substring
  // in the varlen indicator.
  if (getOperand(0)->getVCIndicatorLength() > 0)
    getOperand(0)->setVarLength(endPos - startPos, op_data[-MAX_OPERANDS]);
  else {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ExFunctionReverseStr::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  char *tgt = op_data[0];
  char *src = op_data[1];
  int srcPos = 0;
  int tgtPos = 0;
  if (cs == CharInfo::ISO88591) {
    tgtPos = len1 - 1;
    for (srcPos = 0; srcPos < len1; srcPos++) {
      tgt[tgtPos--] = src[srcPos];
    }
  } else if (cs == CharInfo::UCS2) {
    int bpc = unicode_char_set::bytesPerChar();
    srcPos = 0;
    tgtPos = len1 - bpc;
    while (srcPos < len1) {
      str_cpy_all(&tgt[tgtPos], &src[srcPos], bpc);
      tgtPos -= bpc;
      srcPos += bpc;
    }
  } else if (cs == CharInfo::UTF8) {
    UInt32 UCS4value;

    cnv_charset charset = convertCharsetEnum(cs);
    int charLen;
    srcPos = 0;
    tgtPos = len1;
    while (srcPos < len1) {
      charLen = LocaleCharToUCS4(&op_data[1][srcPos], len1 - srcPos, &UCS4value, charset);
      if (charLen < 0) {
        const char *csname = CharInfo::getCharSetName(cs);
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
        *(*diagsArea) << DgString0(csname) << DgString1("REVERSE FUNCTION");
        return ex_expr::EXPR_ERROR;
      }

      tgtPos -= charLen;
      str_cpy_all(&tgt[tgtPos], &src[srcPos], charLen);
      srcPos += charLen;
    }
  } else {
    const char *csname = CharInfo::getCharSetName(cs);
    ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
    *(*diagsArea) << DgString0(csname) << DgString1("REVERSE FUNCTION");
    return ex_expr::EXPR_ERROR;
  }

  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(len1, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ex_function_sleep::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int sec = 0;
  switch (getOperand(1)->getDatatype()) {
    case REC_BIN8_SIGNED:
      sec = *(Int8 *)op_data[1];
      if (sec < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        *(*diagsArea) << DgString0("SLEEP");
        return ex_expr::EXPR_ERROR;
      }
      sleep(sec);
      *(long *)op_data[0] = 1;
      break;

    case REC_BIN16_SIGNED:
      sec = *(short *)op_data[1];
      if (sec < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        *(*diagsArea) << DgString0("SLEEP");
        return ex_expr::EXPR_ERROR;
      }
      sleep(sec);
      *(long *)op_data[0] = 1;
      break;

    case REC_BIN32_SIGNED:
      sec = *(int *)op_data[1];
      if (sec < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        *(*diagsArea) << DgString0("SLEEP");
        return ex_expr::EXPR_ERROR;
      }
      sleep(sec);
      *(long *)op_data[0] = 1;
      break;

    case REC_BIN64_SIGNED:
      sec = *(long *)op_data[1];
      if (sec < 0) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        *(*diagsArea) << DgString0("SLEEP");
        return ex_expr::EXPR_ERROR;
      }
      sleep(sec);
      *(long *)op_data[0] = 1;
      break;

    default:
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      *(*diagsArea) << DgString0("SLEEP");
      return ex_expr::EXPR_ERROR;
      break;
  }
  // get the seconds to sleep
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_unixtime::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *opData = op_data[1];
  // if there is input value
  if (getNumOperands() == 2) {
    struct tm ptr;
    if (opData == NULL) {
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      *(*diagsArea) << DgString0("UNIX_TIMESTAMP");
      return ex_expr::EXPR_ERROR;
    } else {
      char *r = strptime(opData, "%Y-%m-%d %H:%M:%S", &ptr);
      if ((r == NULL) || (*r != '\0')) {
        ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
        *(*diagsArea) << DgString0("UNIX_TIMESTAMP");
        return ex_expr::EXPR_ERROR;
      } else
        *(long *)op_data[0] = mktime(&ptr);
    }
  } else {
    time_t seconds;
    seconds = time((time_t *)NULL);
    *(long *)op_data[0] = seconds;
  }
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_split_part::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  size_t sourceLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  size_t patternLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  int indexOfTarget = *(int *)op_data[3];

  if (indexOfTarget <= 0) {
    ExRaiseSqlError(heap, diagsArea, EXE_INVALID_FIELD_POSITION);
    *(*diagsArea) << DgInt0(indexOfTarget);
    return ex_expr::EXPR_ERROR;
  }

  NAString source(op_data[1], sourceLen);
  NAString pattern(op_data[2], patternLen);

  int patternCnt = 0;
  StringPos currentTargetPos = 0;
  StringPos pos = 0;

  while (patternCnt != indexOfTarget) {
    currentTargetPos = pos;
    pos = source.index(pattern, pos);
    if (pos == NA_NPOS) break;
    pos = pos + patternLen;
    patternCnt++;
  }

  size_t targetLen = 0;
  if ((patternCnt == 0) || ((patternCnt != indexOfTarget) && (patternCnt != indexOfTarget - 1)))
    op_data[0][0] = '\0';
  else {
    if (patternCnt == indexOfTarget)
      targetLen = pos - currentTargetPos - patternLen;
    else  // if (patternLen == indexOfTarget-1)
      targetLen = sourceLen - currentTargetPos;

    str_cpy_all(op_data[0], op_data[1] + currentTargetPos, targetLen);
  }
  getOperand(0)->setVarLength(targetLen, op_data[-MAX_OPERANDS]);
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_current::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  if (getOperand()) {
    ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(0);

    rec_datetime_field srcStartField;
    rec_datetime_field srcEndField;

    if (datetimeOpType->getDatetimeFields(datetimeOpType->getPrecision(), srcStartField, srcEndField) != 0) {
      return ex_expr::EXPR_ERROR;
    }

    ExpDatetime::currentTimeStamp(op_data[0], srcStartField, srcEndField, datetimeOpType->getScale());
  } else {
    ExpDatetime::currentTimeStamp(op_data[0], REC_DATE_YEAR, REC_DATE_SECOND, ExpDatetime::MAX_DATETIME_FRACT_PREC);
  }

  return ex_expr::EXPR_OK;
};

// MV,
ex_expr::exp_return_type ExFunctionGenericUpdateOutput::eval(char *op_data[], CollHeap *heap,
                                                             ComDiagsArea **diagsArea) {
  // We do not set the value here.
  // The return value is written into the space allocated for it by the
  // executor work method.
  // The value is initialized to zero here in case VSBB is rejected by the
  // optimizer, so the executor will not override this value.
  if (origFunctionOperType() == ITM_VSBBROWCOUNT)
    *(int *)op_data[0] = 1;  // Simple Insert RowCount - 1 row.
  else
    *(int *)op_data[0] = 0;  // Simple Insert RowType is 0.

  return ex_expr::EXPR_OK;
}

// Triggers -
ex_expr::exp_return_type ExFunctionInternalTimestamp::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_function_current currentFun;
  return (currentFun.eval(op_data, heap, diagsArea));
}

ex_expr::exp_return_type ex_function_bool::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;

  switch (getOperType()) {
    case ITM_RETURN_TRUE: {
      *(int *)op_data[0] = 1;
    } break;

    case ITM_RETURN_FALSE: {
      *(int *)op_data[0] = 0;
    } break;

    case ITM_RETURN_NULL: {
      *(int *)op_data[0] = -1;
    } break;

    default: {
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

      retcode = ex_expr::EXPR_ERROR;
    } break;
  }

  return retcode;
}

ex_expr::exp_return_type ex_function_converttimestamp::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  long juliantimestamp;
  str_cpy_all((char *)&juliantimestamp, op_data[1], sizeof(juliantimestamp));
  const long minJuliantimestamp = (long)1487311632 * (long)100000000;
  const long maxJuliantimestamp = (long)2749273487LL * (long)100000000 + (long)99999999;
  if ((juliantimestamp < minJuliantimestamp) || (juliantimestamp > maxJuliantimestamp)) {
    char tmpbuf[24];
    memset(tmpbuf, 0, sizeof(tmpbuf));
    sprintf(tmpbuf, "%ld", juliantimestamp);

    ExRaiseSqlError(heap, diagsArea, EXE_CONVERTTIMESTAMP_ERROR);
    if (*diagsArea) **diagsArea << DgString0(tmpbuf);

    if (derivedFunction()) {
      **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
      **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
    }

    return ex_expr::EXPR_ERROR;
  }

  if (toLocalTime()) juliantimestamp -= gmtDiff_;

  char *datetimeOpData = op_data[0];
  convertJulianTimestamp(juliantimestamp, datetimeOpData);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_dateformat::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *opData = op_data[1];
  char *pFormat = op_data[2];
  int nFormatStrLen = (getNumOperands() > 2) ? getOperand(2)->getLength() : 0;
  char *formatStr = new (heap) char[nFormatStrLen + 1];
  if (nFormatStrLen) {
    memcpy(formatStr, pFormat, nFormatStrLen);
  }
  formatStr[nFormatStrLen] = 0;

  char *result = op_data[0];

  if ((getDateFormat() == ExpDatetime::DATETIME_FORMAT_NUM1) ||
      (getDateFormat() == ExpDatetime::DATETIME_FORMAT_NUM2)) {
    // numeric to TIME conversion.
    if (ExpDatetime::convNumericTimeToASCII(opData, result, getOperand(0)->getLength(), getDateFormat(), formatStr,
                                            heap, diagsArea) < 0) {
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

      return ex_expr::EXPR_ERROR;
    }

  } else {
    // Convert the given datetime value to an ASCII string value in the
    // given format.
    //
    if ((DFS2REC::isAnyCharacter(getOperand(1)->getDatatype())) &&
        (DFS2REC::isDateTime(getOperand(0)->getDatatype()))) {
      int sourceLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

      ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(0);
      if (datetimeOpType->convAsciiToDate(opData, sourceLen, result, getOperand(0)->getLength(), getDateFormat(), heap,
                                          diagsArea, 0) < 0) {
        if (diagsArea && (!(*diagsArea) || ((*diagsArea) && (*diagsArea)->getNumber(DgSqlCode::ERROR_) == 0))) {
          // we expect convAsciiToDate to raise a diagnostic; if it
          // didn't, raise an internal error here
          ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());
        }
        return ex_expr::EXPR_ERROR;
      }
    } else {
      ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(1);
      if (datetimeOpType->convDatetimeToASCII(opData, result, getOperand(0)->getLength(), getDateFormat(), formatStr,
                                              heap, diagsArea, getCaseSensitivity(),
                                              getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1])) < 0) {
        ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

        return ex_expr::EXPR_ERROR;
      }
    }
  }

  if (getOperand(0)->getVCIndicatorLength() > 0) {
    int len = getOperand(0)->getLength();
    getOperand(0)->setVarLength(len, op_data[-MAX_OPERANDS]);
  }
  return ex_expr::EXPR_OK;
}

// Remove all the trailing space.
static void lcl_rtrim(char *buf) {
  char *p;
  p = buf + strlen(buf) - 1;
  while (isspace(*p) && p > buf) *p-- = 0;
}

// translate a value of NUMERIC,INT,FLOAT to varchar
// now we just support the precision of operand1 less than 18
ex_expr::exp_return_type ex_function_numberformat::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *arg1 = getOperand(1);
  Attributes *arg2 = getOperand(2);
  Attributes *arg0 = getOperand(0);

  char *arg1Str = op_data[1];
  char *arg2Str = op_data[2];
  char *resultStr = op_data[0];

  if (arg1->getLength(op_data[-MAX_OPERANDS + 1]) >
      MAX_NUMFORMAT_STR_LEN) {  // The length of op_data[1] should not exceed MAX_NUMFORMAT_STR_LEN
                                // If so , return '#############'
    memset(resultStr, '#', arg0->getLength());
    *(resultStr + arg0->getLength() - 1) = '\0';
    if (getOperand(0)->getVCIndicatorLength() > 0) {
      int len = strlen(resultStr);  // getOperand(0)->getLength();
      getOperand(0)->setVarLength(len, op_data[-MAX_OPERANDS]);
    }
    return ex_expr::EXPR_OK;
  }

  if (arg2->getLength() > MAX_NUMFORMAT_STR_LEN) {  // The length of op_data[2] should not exceed MAX_NUMFORMAT_STR_LEN
                                                    // If so, return error
    ExRaiseSqlError(heap, diagsArea, EXE_TOCHAR_INVALID_FORMAT_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  char tmpStr[MAX_NUMFORMAT_STR_LEN + 1];
  memset(tmpStr, ' ', MAX_NUMFORMAT_STR_LEN);
  int flags = CONV_NOT_TRIM_TAIL_ZERO;
  if (::convDoIt(arg1Str, arg1->getLength(op_data[-MAX_OPERANDS + 1]), arg1->getDatatype(), arg1->getPrecision(),
                 arg1->getScale(), tmpStr, MAX_NUMFORMAT_STR_LEN, REC_BYTE_F_ASCII, 0, 0, op_data[-MAX_OPERANDS], 0,
                 heap, diagsArea, CONV_UNKNOWN, 0, flags) != ex_expr::EXPR_OK)
    return ex_expr::EXPR_ERROR;

  tmpStr[MAX_NUMFORMAT_STR_LEN] = 0;
  lcl_rtrim(tmpStr);

  memset(resultStr, 0, arg0->getLength());
  int nDataType = arg1->getDatatype();
  if (REC_NUM_BIG_SIGNED == nDataType) {
    if (ExpNumerFormat::convertBigNumToChar(tmpStr, resultStr, arg0, arg1, arg2, arg1Str, arg2Str, heap, diagsArea))
      return ex_expr::EXPR_ERROR;
  } else if (0 != arg1->getScale()) {
    if (ExpNumerFormat::convertFloatToChar(tmpStr, resultStr, arg0, arg1, arg2, arg1Str, arg2Str, heap, diagsArea))
      return ex_expr::EXPR_ERROR;
  } else if (REC_BIN8_SIGNED == nDataType || REC_BIN8_UNSIGNED == nDataType || REC_BIN16_SIGNED == nDataType ||
             REC_BIN16_UNSIGNED == nDataType || REC_BIN32_SIGNED == nDataType || REC_BIN32_UNSIGNED == nDataType) {
    if (ExpNumerFormat::convertInt32ToChar(tmpStr, resultStr, arg0, arg1, arg2, arg1Str, arg2Str, heap, diagsArea))
      return ex_expr::EXPR_ERROR;
  } else if (REC_BIN64_SIGNED == nDataType) {
    if (ExpNumerFormat::convertInt64ToChar(tmpStr, resultStr, arg0, arg1, arg2, arg1Str, arg2Str, heap, diagsArea))
      return ex_expr::EXPR_ERROR;
  } else {
    ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
    **diagsArea << DgString0("TO_CHAR");
    return ex_expr::EXPR_ERROR;
  }

  if (getOperand(0)->getVCIndicatorLength() > 0) {
    int len = strlen(resultStr);  // getOperand(0)->getLength();
    getOperand(0)->setVarLength(len, op_data[-MAX_OPERANDS]);
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_dayofweek::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  long interval;
  short year;
  char month;
  char day;
  ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(1);
  char *datetimeOpData = op_data[1];
  str_cpy_all((char *)&year, datetimeOpData, sizeof(year));
  datetimeOpData += sizeof(year);
  month = *datetimeOpData++;
  day = *datetimeOpData;
  interval = datetimeOpType->getTotalDays(year, month, day);
  unsigned short result = (unsigned short)((interval + 1) % 7) + 1;
  str_cpy_all(op_data[0], (char *)&result, sizeof(result));
  return ex_expr::EXPR_OK;
}

static long lcl_dayofweek(long totaldays) { return (unsigned short)((totaldays + 1) % 7) + 1; }

static int lcl_date2Julian(int y, int m, int d) {
  int myjulian = 0;
  int mycentury = 0;
  if (m <= 2) {
    m = m + 13;
    y = y + 4799;
  } else {
    m = m + 1;
    y = y + 4800;
  }

  myjulian = y / 100;
  myjulian = y * 365 - 32167;
  myjulian += y / 4 - mycentury + mycentury / 4;
  myjulian += 7834 * m / 256 + d;
  return myjulian;
}

static long lcl_dayofyear(char year, char month, char day) {
  return (lcl_date2Julian(year, month, day) - lcl_date2Julian(year, 1, 1) + 1);
}

#define DAYS_PER_YEAR      365.25 /*consider leap year every four years*/
#define MONTHS_PER_YEAR    12
#define DAYS_PER_MONTH     30
#define HOURS_PER_DAY      24
#define SECONDS_PER_MINUTE 60
#define SECONDS_PER_HOUR   3600
#define SECONDS_PER_DAY    86400

static long lcl_interval(rec_datetime_field eField, int eCode, char *opdata, UInt32 nLength, Int16 nScale) {
  if (!opdata) return 0;
  long nVal = 0;
  if (SQL_SMALL_SIZE == nLength) {
    short v;
    str_cpy_all((char *)&v, opdata, sizeof(v));
    nVal = v;
  } else if (SQL_INT_SIZE == nLength) {
    int v;
    str_cpy_all((char *)&v, opdata, sizeof(v));
    nVal = v;
  } else if (SQL_LARGE_SIZE == nLength) {
    str_cpy_all((char *)&nVal, opdata, sizeof(nVal));
  }

  if (REC_DATE_DECADE == eField) {
    if (REC_INT_YEAR == eCode) {
      return nVal / 10;
    } else if (REC_INT_MONTH == eCode || REC_INT_YEAR_MONTH == eCode) {
      return nVal / MONTHS_PER_YEAR / 10;
    } else if (REC_INT_DAY == eCode) {
      return nVal / DAYS_PER_YEAR / 10;
    } else if (REC_INT_HOUR == eCode || REC_INT_DAY_HOUR == eCode) {
      return nVal / HOURS_PER_DAY / DAYS_PER_YEAR / 10;
    } else if (REC_INT_MINUTE == eCode || REC_INT_HOUR_MINUTE == eCode || REC_INT_DAY_MINUTE == eCode) {
      return nVal / 60 / HOURS_PER_DAY / DAYS_PER_YEAR / 10;
      ;
    } else if (REC_INT_SECOND == eCode || REC_INT_MINUTE_SECOND == eCode || REC_INT_HOUR_SECOND == eCode ||
               REC_INT_DAY_SECOND == eCode) {
      UInt32 p = 1;
      if (nScale > 0) p = pow(10, nScale);
      return nVal / SECONDS_PER_DAY / DAYS_PER_YEAR / p / 10;
    }
  }
  if (REC_DATE_QUARTER == eField && REC_INT_MONTH == eCode) {
    short nValue;
    str_cpy_all((char *)&nValue, opdata, sizeof(nValue));
    if (nValue <= 0) return 0;
    return (nValue - 1) / 3 + 1;
  }
  if (REC_DATE_EPOCH == eField) {
    if (REC_INT_YEAR == eCode)
      return nVal * DAYS_PER_YEAR * SECONDS_PER_DAY;
    else if (REC_INT_MONTH == eCode || REC_INT_YEAR_MONTH == eCode) {
      double result = (double)(nVal / MONTHS_PER_YEAR) * DAYS_PER_YEAR * SECONDS_PER_DAY;
      result += (double)(nVal % MONTHS_PER_YEAR) * DAYS_PER_MONTH * SECONDS_PER_DAY;
      return long(result);
    } else if (REC_INT_DAY == eCode)
      return nVal * SECONDS_PER_DAY;
    else if (REC_INT_HOUR == eCode || REC_INT_DAY_HOUR == eCode)
      return nVal * SECONDS_PER_HOUR;
    else if (REC_INT_MINUTE == eCode || REC_INT_HOUR_MINUTE == eCode || REC_INT_DAY_MINUTE == eCode)
      return nVal * SECONDS_PER_MINUTE;
    else if (REC_INT_SECOND == eCode || REC_INT_MINUTE_SECOND == eCode || REC_INT_HOUR_SECOND == eCode ||
             REC_INT_DAY_SECOND == eCode)
      return nVal;
  }
  return 0;
}

long ex_function_extract::getExtraTimeValue(rec_datetime_field eField, int eCode, char *dateTime) {
  short year = 0;
  char month = 0;
  char day = 0;
  char hour = 0;
  char minute = 0;
  char second = 0;
  int millisecond = 0;
  if (eField < REC_DATE_CENTURY || eField > REC_DATE_WOM) return 0;
  if (eCode != REC_DTCODE_DATE && eCode != REC_DTCODE_TIME && eCode != REC_DTCODE_TIMESTAMP) return 0;

  ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(1);
  if (!datetimeOpType) return 0;

  int field = REC_DATE_YEAR;
  if (REC_DTCODE_TIME == eCode) field = REC_DATE_HOUR;
  rec_datetime_field eEndFiled = REC_DATE_DAY;
  if (REC_DTCODE_TIMESTAMP == eCode || REC_DTCODE_TIME == eCode) eEndFiled = REC_DATE_SECOND;
  size_t n = strlen(dateTime);
  for (; field <= eEndFiled; field++) {
    switch (field) {
      case REC_DATE_YEAR: {
        str_cpy_all((char *)&year, dateTime, sizeof(year));
        dateTime += sizeof(year);
      } break;
      case REC_DATE_MONTH: {
        month = *dateTime++;
      } break;
      case REC_DATE_DAY: {
        day = *dateTime;
        if (REC_DATE_SECOND == eEndFiled) dateTime++;
      } break;
      case REC_DATE_HOUR: {
        hour = *dateTime++;
      } break;
      case REC_DATE_MINUTE: {
        minute = *dateTime++;
      } break;
      case REC_DATE_SECOND: {
        second = *dateTime;
        if (n > 7)  // 2018-06-20 20:30:15.12  length = 8
        {
          dateTime++;
          str_cpy_all((char *)&millisecond, dateTime, n - 7);
        }
        if (REC_DTCODE_TIME == eCode) {
          if (n > 3) {
            dateTime++;
            str_cpy_all((char *)&millisecond, dateTime, n - 3);
          }
        }
      } break;
    }
  }
  switch (eField) {
    case REC_DATE_DOW: {  // same with built-in function dayofweek  ex_function_dayofweek::eval
      long interval = datetimeOpType->getTotalDays(year, month, day);
      return lcl_dayofweek(interval);
    }
    case REC_DATE_DOY: {
      return lcl_dayofyear(year, month, day);
    }
    case REC_DATE_WOM: {
      /*long interval = datetimeOpType->getTotalDays(year, month, 1);
      long firstdayinmonthofweek = lcl_dayofweek(interval);
      int exceptday = 0;
      if (firstdayinmonthofweek != 2)
        exceptday = 7 - firstdayinmonthofweek + 2;

      return (day - exceptday)/7 + (day < exceptday ? 0 : 1);*/
      return ((day - 1) / 7 + 1);
    }
    case REC_DATE_CENTURY: {
      return (year + 99) / 100;
    }
    case REC_DATE_DECADE: {
      return year / 10;
    }
    case REC_DATE_WEEK: {  // same with built-in function week  ITM_WEEK
      long interval = datetimeOpType->getTotalDays(year, 1, 1);
      long dayofweek = lcl_dayofweek(interval);
      long dayofyear = lcl_dayofyear(year, month, day);
      return (dayofyear - 1 + dayofweek - 1) / 7 + 1;
    }
    case REC_DATE_QUARTER: {
      return (month - 1) / 3 + 1;
    }
    case REC_DATE_EPOCH: {
      long ndays = datetimeOpType->getTotalDays(year, month, day);
      if (ndays > 0) {
        long nJuliandays = datetimeOpType->getTotalDays(1970, 1, 1);
        ndays = ndays - nJuliandays;
      } else
        ndays = 0;
      long ntimestamp = ndays * 86400 + hour * 3600 + minute * 60 + second;
      if (0 != millisecond) {
        Int16 nScale = datetimeOpType->getScale();
        UInt32 p = 1;
        if (nScale > 0) p = pow(10, nScale);
        ntimestamp = ntimestamp * p + millisecond;
      }
      return ntimestamp;
    }
  }
  return 0;
}

ex_expr::exp_return_type ex_function_extract::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  long result = 0;
  if (getOperand(1)->getDatatype() == REC_DATETIME) {
    ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(1);
    char *datetimeOpData = op_data[1];
    rec_datetime_field opStartField;
    rec_datetime_field opEndField;
    rec_datetime_field extractStartField = getExtractField();
    rec_datetime_field extractEndField = extractStartField;

    if (extractStartField >= REC_DATE_CENTURY && extractStartField <= REC_DATE_WOM) {
      result = getExtraTimeValue(extractStartField, datetimeOpType->getPrecision(), datetimeOpData);
      copyInteger(op_data[0], getOperand(0)->getLength(), &result, sizeof(result));
      return ex_expr::EXPR_OK;
    }

    if (extractStartField > REC_DATE_MAX_SINGLE_FIELD) {
      extractStartField = REC_DATE_YEAR;
      if (extractEndField == REC_DATE_YEARQUARTER_EXTRACT || extractEndField == REC_DATE_YEARMONTH_EXTRACT ||
          extractEndField == REC_DATE_YEARQUARTER_D_EXTRACT || extractEndField == REC_DATE_YEARMONTH_D_EXTRACT)
        extractEndField = REC_DATE_MONTH;
      else {
        ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());
        return ex_expr::EXPR_ERROR;
      }
    }
    if (datetimeOpType->getDatetimeFields(datetimeOpType->getPrecision(), opStartField, opEndField) != 0) {
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

      return ex_expr::EXPR_ERROR;
    }
    for (int field = opStartField; field <= extractEndField; field++) {
      switch (field) {
        case REC_DATE_YEAR: {
          short value;
          if (field >= extractStartField && field <= extractEndField) {
            str_cpy_all((char *)&value, datetimeOpData, sizeof(value));
            result = value;
          }
          datetimeOpData += sizeof(value);
          break;
        }
        case REC_DATE_MONTH:
        case REC_DATE_DAY:
        case REC_DATE_HOUR:
        case REC_DATE_MINUTE:
          if (field >= extractStartField && field <= extractEndField) {
            switch (getExtractField()) {
              case REC_DATE_YEARQUARTER_EXTRACT:
                // 10*year + quarter - human readable quarter format
                result = 10 * result + ((*datetimeOpData) + 2) / 3;
                break;
              case REC_DATE_YEARQUARTER_D_EXTRACT:
                // 4*year + 0-based quarter - dense quarter format, better for MDAM
                result = 4 * result + (*datetimeOpData - 1) / 3;
                break;
              case REC_DATE_YEARMONTH_EXTRACT:
                // 100*year + month - human readable yearmonth format
                result = 100 * result + *datetimeOpData;
                break;
              case REC_DATE_YEARMONTH_D_EXTRACT:
                // 12*year + 0-based month - dense month format, better for MDAM
                result = 12 * result + *datetimeOpData - 1;
                break;
              default:
                // regular extract of month, day, hour, minute
                result = *datetimeOpData;
                break;
            }
          }
          datetimeOpData++;
          break;
        case REC_DATE_SECOND:
          if (field == getExtractField()) {
            result = *datetimeOpData;
            datetimeOpData++;
            short fractionPrecision = datetimeOpType->getScale();
            if (fractionPrecision > 0) {
              do {
                result *= 10;
              } while (--fractionPrecision > 0);
              int fraction;
              str_cpy_all((char *)&fraction, datetimeOpData, sizeof(fraction));
              result += fraction;
            }
          }
          break;
        default:
          ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

          return ex_expr::EXPR_ERROR;
      }
    }
  } else {
    if (getExtractField() == REC_DATE_DECADE || getExtractField() == REC_DATE_QUARTER ||
        getExtractField() == REC_DATE_EPOCH) {
      ExpDatetime *datetimeOpType = (ExpDatetime *)getOperand(1);
      result = lcl_interval(getExtractField(), getOperand(1)->getDatatype(), op_data[1], getOperand(1)->getLength(),
                            datetimeOpType->getScale());
      copyInteger(op_data[0], getOperand(0)->getLength(), &result, sizeof(result));
      return ex_expr::EXPR_OK;
    }
    long interval;
    switch (getOperand(1)->getLength()) {
      case SQL_SMALL_SIZE: {
        short value;
        str_cpy_all((char *)&value, op_data[1], sizeof(value));
        interval = value;
        break;
      }
      case SQL_INT_SIZE: {
        int value;
        str_cpy_all((char *)&value, op_data[1], sizeof(value));
        interval = value;
        break;
      }
      case SQL_LARGE_SIZE:
        str_cpy_all((char *)&interval, op_data[1], sizeof(interval));
        break;
      default:
        ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
        return ex_expr::EXPR_ERROR;
    }
    rec_datetime_field startField;
    if (ExpInterval::getIntervalStartField(getOperand(1)->getDatatype(), startField) != 0) return ex_expr::EXPR_ERROR;
    if (getExtractField() == startField)
      result = interval;
    else {
      switch (getExtractField()) {
        case REC_DATE_MONTH:
          //
          // The sign of the result of a modulus operation involving a negative
          // operand is implementation-dependent according to the C++ reference
          // manual.  In this case, we prefer the result to be negative.
          //
          result = interval % 12;
          if ((interval < 0) && (result > 0)) result = -result;
          break;
        case REC_DATE_HOUR:
          //
          // The sign of the result of a modulus operation involving a negative
          // operand is implementation-dependent according to the C++ reference
          // manual.  In this case, we prefer the result to be negative.
          //
          result = interval % 24;
          if ((interval < 0) && (result > 0)) result = -result;
          break;
        case REC_DATE_MINUTE:
          //
          // The sign of the result of a modulus operation involving a negative
          // operand is implementation-dependent according to the C++ reference
          // manual.  In this case, we prefer the result to be negative.
          //
          result = interval % 60;
          if ((interval < 0) && (result > 0)) result = -result;
          break;
        case REC_DATE_SECOND: {
          int divisor = 60;
          for (short fp = getOperand(1)->getScale(); fp > 0; fp--) divisor *= 10;
          result = interval;
          interval = result / (long)divisor;
          result -= interval * (long)divisor;
          break;
        }
        default:
          ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
          return ex_expr::EXPR_ERROR;
      }
    }
  }

  copyInteger(op_data[0], getOperand(0)->getLength(), &result, sizeof(result));

  return ex_expr::EXPR_OK;
}

static NABoolean isLeapYear(int year) { return ((year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0))); }

static NABoolean isLastDayOfMonth(short year, char month, char day) {
  switch (month) {
    case 1:
    case 3:
    case 5:
    case 7:
    case 8:
    case 10:
    case 12:
      return (day == 31);
    case 4:
    case 6:
    case 9:
    case 11:
      return (day == 30);
    case 2:
      if (isLeapYear(year) && day == 29)
        return TRUE;
      else if (!isLeapYear(year) && day == 28)
        return TRUE;
      return FALSE;
    default:
      return FALSE;
  }
}

// Returns number of months between dates date1 and date2
// If date1 is later than date2, then the result is positive.
// If date1 is earlier than date2, then the result is negative.
// If date1 and date2 are either the same days of the month
// or both last days of months, then the result is always an integer.
// Otherwise calculates the fractional portion of the
// result based on a 31-day month and considers the difference
// in time components date1 and date2.
ex_expr::exp_return_type ex_function_monthsbetween::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  memset(op_data[0], 0, getOperand(0)->getLength());
  short year1 = 0;
  char month1 = 0;
  char day1 = 0;
  short year2 = 0;
  char month2 = 0;
  char day2 = 0;

  long result = 0;
  int nLen = sizeof(year1) + sizeof(month1) + sizeof(day1);
  if (getOperand(1)->getLength() == nLen && getOperand(2)->getLength() == nLen) {
    char *opData1 = op_data[1];
    char *opData2 = op_data[2];

    str_cpy_all((char *)&year1, opData1, sizeof(year1));
    opData1 += sizeof(year1);
    str_cpy_all((char *)&year2, opData2, sizeof(year2));
    opData2 += sizeof(year2);

    month1 = *opData1++;
    month2 = *opData2++;
    day1 = *opData1++;
    day2 = *opData2++;

    NABoolean lastDayOfMonth1 = isLastDayOfMonth(year1, month1, day1);
    NABoolean lastDayOfMonth2 = isLastDayOfMonth(year2, month2, day2);
    NABoolean bSameOrLastDay = (day1 == day2 || (lastDayOfMonth1 && lastDayOfMonth2));
    result = (year1 - year2) * 12 + (month1 - month2);
    result *= 1000000000;  // add decimal digits(.000000000) to convert to numeric(15,9)
    if (!bSameOrLastDay) {
      // calculates the fractional portion of the result based on a 31-day month
      double d = double((day1 - day2)) / 31;
      result = result + d * 1000000000;
    }
    *(long *)op_data[0] = result;
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_juliantimestamp::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type retcode = ex_expr::EXPR_OK;
  long juliantimestamp;

  char *datetimeOpData = op_data[1];
  short year;
  char month;
  char day;
  char hour;
  char minute;
  char second;
  int fraction;
  str_cpy_all((char *)&year, datetimeOpData, sizeof(year));
  datetimeOpData += sizeof(year);
  month = *datetimeOpData++;
  day = *datetimeOpData++;
  hour = *datetimeOpData++;
  minute = *datetimeOpData++;
  second = *datetimeOpData++;
  str_cpy_all((char *)&fraction, datetimeOpData, sizeof(fraction));
  short timestamp[] = {year, month, day, hour, minute, second, (short)(fraction / 1000), (short)(fraction % 1000)};
  short error;
  juliantimestamp = COMPUTETIMESTAMP(timestamp, &error);
  if (error) {
    char tmpbuf[24];
    memset(tmpbuf, 0, sizeof(tmpbuf));
    sprintf(tmpbuf, "%ld", juliantimestamp);

    ExRaiseSqlError(heap, diagsArea, EXE_JULIANTIMESTAMP_ERROR);
    if (*diagsArea) **diagsArea << DgString0(tmpbuf);

    if (derivedFunction()) {
      **diagsArea << DgSqlCode(-EXE_MAPPED_FUNCTION_ERROR);
      **diagsArea << DgString0(exClauseGetText(origFunctionOperType()));
    }

    return ex_expr::EXPR_ERROR;
  }

  str_cpy_all(op_data[0], (char *)&juliantimestamp, sizeof(juliantimestamp));
  return retcode;
}

ex_expr::exp_return_type ex_function_exec_count::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  execCount_++;
  str_cpy_all(op_data[0], (char *)&execCount_, sizeof(execCount_));
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_curr_transid::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // this function is not used yet anywhere, whoever wants to start using
  // it should fill in the missing code here
  ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

  return ex_expr::EXPR_ERROR;
}

// -----------------------------------------------------------------------
// Helper function for CURRENT_USER and SESSION_USER function.
// Used by exp and UDR code to get the CURRENT_USER and SESSION_USER
// information. SESSION_USER is the user that is logged on to the
// current SQL session. CURRENT_USER is the one with whose privileges
// a SQL statement is executed, With Definer Rights SPJ, the CURRENT_USER is
// the owner of the SPJ while SESSION_USER is the user who invoked the SPJ.
//
// Returns the current login user name as a C-style string (null terminated)
// in inputUserNameBuffer parameter.
// (e.g. returns "Domain1\Administrator" on NT if logged
//                    in as Domain1\Administrator,
//               "role-mgr" on NSK if logged in as alias "role-mgr",
//               "ROLE.MGR" on NSK if logged in as Guardian userid ROLE.MGR)
// Returns 0 as the return value on success
// Returns 100 if not found
// Returns -1 for unexpected errors
//
// Optionally returns the actual length of the user name (in bytes) in
// actualLength parameter. Returns 0 as the actual length if the function returns
// an error code, except for when the buffer is too small in which case it
// returns the actual length so that the caller can get an idea of the minimum
// size of the input buffer to be provided.
// -----------------------------------------------------------------------
short exp_function_get_user(OperatorTypeEnum userType,  // IN - CURRENT_USER or SESSION_USER
                            char *inputUserNameBuffer,  // IN - buffer for returning the user name
                            int inputBufferLength,      // IN - length(max) of the above buffer in bytes
                            int *actualLength)          // OUT optional - actual length of the user name
{
  if (actualLength) *actualLength = 0;

  short result = 0;
  int lActualLength = 0;

  int userID;

  if (userType == ITM_SESSION_USER)
    userID = ComUser::getSessionUser();
  else
    userID = ComUser::getCurrentUser();

  char userName[MAX_USERNAME_LEN + 1];
  result = ComUser::getUserNameFromUserID((int)userID, (char *)&userName, (int)inputBufferLength, lActualLength,
                                          NULL /*don't update diags*/);
  if (result == 0) {
    str_cpy_all(inputUserNameBuffer, userName, lActualLength);
    inputUserNameBuffer[lActualLength] = '\0';
  }

  if (actualLength) *actualLength = lActualLength;

  return result;
}

ex_expr::exp_return_type ex_function_ansi_user::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  const int MAX_USER_NAME_LEN = ComSqlId::MAX_LDAP_USER_NAME_LEN;

  char username[MAX_USER_NAME_LEN + 1];
  int username_len = 0;
  short retcode = 0;

  retcode = exp_function_get_user(getOperType(), username, MAX_USER_NAME_LEN + 1, &username_len);

  if (((retcode != 0) && (retcode != 100)) || ((retcode == 0) && (username_len == 0))) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  str_cpy_all(op_data[0], username, username_len);

  getOperand(0)->setVarLength(username_len, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

// -----------------------------------------------------------------------
// Helper function for CURRENT_TENANT .
// Used by exp and UDR code to get the CURRENT_USER
// information.
//
// Returns:
//   tenantName
//   tenantNameLength
//   actualLength
//
// Error codes:
//   0 as the return value on success
//  -1 error
//
//  If actualLength > inputBufferLength, then the buffer is too small
// -----------------------------------------------------------------------
short exp_function_get_tenant(OperatorTypeEnum userType,    // IN - CURRENT_TENANT
                              char *inputTenantNameBuffer,  // IN/OUT - buffer for returning the tenant name
                              int inputBufferLength,        // IN - length(max) of the above buffer in bytes
                              int &actualLength)            // OUT - actual length of the user name
{
  actualLength = 0;

  char *cachedTenantName = (char *)ComTenant::getCurrentTenantName();
  if (cachedTenantName == NULL) return -1;

  int lActualLength = strlen(cachedTenantName);
  if (strlen(cachedTenantName) >= inputBufferLength) return -1;

  actualLength = lActualLength;
  strcpy(inputTenantNameBuffer, cachedTenantName);

  return 0;
}

ex_expr::exp_return_type ex_function_ansi_tenant::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  const int MAX_TENANT_NAME_LEN = MAX_AUTHNAME_LEN;

  char tenantname[MAX_TENANT_NAME_LEN + 1];
  int tenantname_len = 0;

  short retcode = exp_function_get_tenant(getOperType(), tenantname, MAX_TENANT_NAME_LEN + 1, tenantname_len);

  if ((retcode != 0) || ((retcode == 0) && (tenantname_len == 0))) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_TENANT_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  str_cpy_all(op_data[0], tenantname, tenantname_len);

  getOperand(0)->setVarLength(tenantname_len, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}
ex_expr::exp_return_type ex_function_user::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int userIDLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  long id64;

  switch (userIDLen) {
    case SQL_SMALL_SIZE:
      id64 = *((short *)op_data[1]);
      break;
    case SQL_INT_SIZE:
      id64 = *((int *)op_data[1]);
      break;
    case SQL_LARGE_SIZE:
      id64 = *((long *)op_data[1]);
      break;
    default:
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

      return ex_expr::EXPR_ERROR;
  }

  if (id64 < -SQL_INT32_MAX || id64 > SQL_INT32_MAX) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  int authID = (int)(id64);
  // *****************************************************************************
  // *                                                                           *
  // * Code to handle functions AUTHNAME and AUTHTYPE.  Piggybacked on USER      *
  // * function code in parser, binder, and optimizer.  Perhaps there is a       *
  // * better way to implement.                                                  *
  // *                                                                           *
  // * AUTHNAME invokes the porting layer, which calls CLI, as it may be         *
  // * necessary to read metadata (and therefore have a transaction within       *
  // * a transaction).                                                           *
  // *                                                                           *
  // * AUTHTYPE calls Catman directly, as Catman knows the values and ranges     *
  // * for various types of authentication ID values.  Examples include          *
  // * PUBLIC, SYSTEM, users, and roles.  AUTHTYPE returns a single character    *
  // * that can be used within a case, if, or where clause.                      *
  // *                                                                           *
  // *****************************************************************************

  switch (getOperType()) {
    case ITM_AUTHNAME: {
      Int16 result;
      int authNameLen = 0;
      char authName[MAX_AUTHNAME_LEN + 1];

      // clearDiags removes errors from the diags area that may have been
      // generated by getAuthNameFromAuthID.
      NABoolean clearDiags = TRUE;
      result = ComUser::getAuthNameFromAuthID(authID, authName, sizeof(authName), authNameLen, clearDiags,
                                              NULL /*don't update diags*/);

      // If an error occurs getting the AUTHNAME (perhaps authID is not found)
      // and something was returned (authNameLen > 0), then display the
      // returned info.  It should be the string value of the authID.
      if (result != 0 && authNameLen == 0) {
        ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

        return ex_expr::EXPR_ERROR;
      }

      if (authNameLen > getOperand(0)->getLength()) {
        ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

        return ex_expr::EXPR_ERROR;
      }

      getOperand(0)->setVarLength(authNameLen, op_data[-MAX_OPERANDS]);
      str_cpy_all(op_data[0], authName, authNameLen);

      return ex_expr::EXPR_OK;
    }
    case ITM_AUTHTYPE: {
      char authType[2];

      authType[1] = 0;
      authType[0] = ComUser::getAuthType(authID);
      getOperand(0)->setVarLength(1, op_data[-MAX_OPERANDS]);
      str_cpy_all(op_data[0], authType, 1);

      return ex_expr::EXPR_OK;
    }
    case ITM_USER:
    case ITM_USERID:
    default: {
      // Drop down to user code
    }
  }

  int userNameLen = 0;
  char userName[MAX_USERNAME_LEN + 1];

  Int16 result = ComUser::getUserNameFromUserID(authID, (char *)&userName, MAX_USERNAME_LEN + 1, userNameLen,
                                                NULL /*don't update diags*/);

  if ((result != 0) && (result != 100)) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  } else if (result == 100 || userNameLen == 0) {
    // set the user name same as user id
    // avoids exceptions if userID not present in USERS table

    if (authID < 0) {
      userName[0] = '-';
      str_itoa((int)(-authID), &userName[1]);
    } else {
      str_itoa((int)(authID), userName);
    }
    userNameLen = str_len(userName);
  }

  if (userNameLen > getOperand(0)->getLength()) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_USER_FUNCTION_ERROR, derivedFunction(), origFunctionOperType());

    return ex_expr::EXPR_ERROR;
  }

  getOperand(0)->setVarLength(userNameLen, op_data[-MAX_OPERANDS]);
  str_cpy_all(op_data[0], userName, userNameLen);

  return ex_expr::EXPR_OK;
};

////////////////////////////////////////////////////////////////////
//
// encodeKeyValue
//
// This routine encodes key values so that they can be sorted simply
// using binary collation.  The routine is called by the executor.
//
// Note: The target MAY point to the source to change the original
//       value.
//
////////////////////////////////////////////////////////////////////
void ex_function_encode::encodeKeyValue(Attributes *attr, const char *source, const char *varlenPtr, char *target,
                                        NABoolean isCaseInsensitive, Attributes *tgtAttr, char *tgt_varlen_ptr,
                                        const int tgtLength, CharInfo::Collation collation,
                                        CollationInfo::CollationType collType) {
  int fsDatatype = attr->getDatatype();
  int length = attr->getLength();
  int precision = attr->getPrecision();

  switch (fsDatatype) {
#if defined(NA_LITTLE_ENDIAN)
    case REC_BIN8_SIGNED:
      //
      // Flip the sign bit.
      //
      *(UInt8 *)target = *(UInt8 *)source;
      target[0] ^= 0200;
      break;

    case REC_BIN8_UNSIGNED:
    case REC_BOOLEAN:
      *(UInt8 *)target = *(UInt8 *)source;
      break;

    case REC_BIN16_SIGNED:
      //
      // Flip the sign bit.
      //
      *((unsigned short *)target) = reversebytes(*((unsigned short *)source));
      target[0] ^= 0200;
      break;

    case REC_BPINT_UNSIGNED:
    case REC_BIN16_UNSIGNED:
      *((unsigned short *)target) = reversebytes(*((unsigned short *)source));
      break;

    case REC_BIN32_SIGNED:
      //
      // Flip the sign bit.
      //
      *((int *)target) = reversebytes(*((int *)source));
      target[0] ^= 0200;
      break;

    case REC_BIN32_UNSIGNED:
      *((int *)target) = reversebytes(*((int *)source));
      break;

    case REC_BIN64_SIGNED:
      //
      // Flip the sign bit.
      //
      *((long long *)target) = reversebytes(*((long long *)source));
      target[0] ^= 0200;
      break;

    case REC_BIN64_UNSIGNED:
      *((UInt64 *)target) = reversebytes(*((UInt64 *)source));
      break;

    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND:
      switch (length) {
        case 2:  // Signed 16 bit
          *((unsigned short *)target) = reversebytes(*((unsigned short *)source));
          break;
        case 4:  // Signed 32 bit
          *((int *)target) = reversebytes(*((int *)source));
          break;
        case 8:  // Signed 64 bit
          *((long long *)target) = reversebytes(*((long long *)source));
          break;
        default:
          assert(FALSE);
          break;
      };  // switch(length)
      target[0] ^= 0200;
      break;
    case REC_DATETIME: {
      // This method has been modified as part of the MP Datetime
      // Compatibility project.  It has been made more generic so that
      // it depends only on the start and end fields of the datetime type.
      //
      rec_datetime_field startField;
      rec_datetime_field endField;

      ExpDatetime *dtAttr = (ExpDatetime *)attr;

      // Get the start and end fields for this Datetime type.
      //
      dtAttr->getDatetimeFields(dtAttr->getPrecision(), startField, endField);

      // Copy all of the source to the destination, then reverse only
      // those fields of the target that are longer than 1 byte
      //
      if (target != source) str_cpy_all(target, source, length);

      // Reverse the YEAR and Fractional precision fields if present.
      //
      char *ptr = target;
      for (int field = startField; field <= endField; field++) {
        switch (field) {
          case REC_DATE_YEAR:
            // convert YYYY from little endian to big endian
            //
            *((unsigned short *)ptr) = reversebytes(*((unsigned short *)ptr));
            ptr += sizeof(short);
            break;
          case REC_DATE_MONTH:
          case REC_DATE_DAY:
          case REC_DATE_HOUR:
          case REC_DATE_MINUTE:
            // One byte fields are copied as is...
            ptr++;
            break;
          case REC_DATE_SECOND:
            ptr++;

            // if there is a fraction, make it big endian
            // (it is an unsigned long, beginning after the SECOND field)
            //
            if (dtAttr->getScale() > 0) *((int *)ptr) = reversebytes(*((int *)ptr));
            break;
        }
      }
      break;
    }
#else
    case REC_BIN8_SIGNED:
    case REC_BIN16_SIGNED:
    case REC_BIN32_SIGNED:
    case REC_BIN64_SIGNED:
    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND:
      //
      // Flip the sign bit.
      //
      if (target != source) str_cpy_all(target, source, length);
      target[0] ^= 0200;
      break;
#endif

    case REC_DECIMAL_LSE:
      //
      // If the number is negative, complement all the bytes.  Otherwise, set
      // the sign bit.
      //
      if (source[0] & 0200) {
        for (int i = 0; i < length; i++) target[i] = ~source[i];
      } else {
        if (target != source) str_cpy_all(target, source, length);
        target[0] |= 0200;
      }
      break;
    case REC_NUM_BIG_UNSIGNED: {
      BigNum type(length, precision, 0, 1);
      type.encode(source, target);
      break;
    }
    case REC_NUM_BIG_SIGNED: {
      BigNum type(length, precision, 0, 0);
      type.encode(source, target);
      break;
    }
    case REC_IEEE_FLOAT32: {
      //
      // unencoded float (IEEE 754 - 1985 standard):
      //
      // +-+----------+---------------------+
      // | | exponent |  mantissa           |
      // | | (8 bits) |  (23 bits)          |
      // +-+----------+---------------------+
      //  |
      //  +- Sign bit
      //
      // Encoded float (IEEE 754 - 1985 standard):
      //
      // +-+--------+-----------------------+
      // | |Exponent| Mantissa              |
      // | |(8 bits)| (23 bits)             |
      // +-+--------+-----------------------+
      //  ||                                |
      //  |+- Complemented if sign was neg.-+
      //  |
      //  +- Sign bit complement
      //

      // the following code is independent of the "endianess" of the
      // architecture. Instead, it assumes IEEE 754 - 1985 standard
      // for representation of floats

      // source may not be aligned, move it to a temp var.
      float floatsource;
      str_cpy_all((char *)&floatsource, source, length);
      int *dblword = (int *)&floatsource;
      if (floatsource < 0)     // the sign is negative,
        *dblword = ~*dblword;  // flip all the bits
      else
        floatsource = -floatsource;  // complement the sign bit

        // here comes the dependent part
#ifdef NA_LITTLE_ENDIAN
      *(int *)target = reversebytes(*dblword);
#else
      //    *(unsigned long *) target = *dblword;
      str_cpy_all(target, (char *)&floatsource, length);
#endif
      break;
    }
    case REC_IEEE_FLOAT64: {
      //
      // unencoded double (IEEE 754 - 1985 standard):
      //
      // +-+--------- -+--------------------+
      // | | exponent  |  mantissa          |
      // | | (11 bits) |  (52 bits)         |
      // +-+--------- -+--------------------+
      //  |
      //  +- Sign bit
      //
      // Encoded double (IEEE 754 - 1985 standard):
      //
      // +-+-----------+--------------------+
      // | | Exponent  | Mantissa           |
      // | | (11 bits) | (52 bits)          |
      // +-+-----------+--------------------+
      //  ||                                |
      //  |+- Complemented if sign was neg.-+
      //  |
      //  +- Sign bit complement
      //

      // the following code is independent of the "endianess" of the
      // archtiecture. Instead, it assumes IEEE 754 - 1985 standard
      // for representation of floats

      // double doublesource = *(double *) source;

      // source may not be aligned, move it to a temp var.
      double doublesource;
      str_cpy_all((char *)&doublesource, source, length);

      long *quadword = (long *)&doublesource;
      if (doublesource < 0)      // the sign is negative,
        *quadword = ~*quadword;  // flip all the bits
      else
        doublesource = -doublesource;  // complement the sign bit

        // here comes the dependent part
#ifdef NA_LITTLE_ENDIAN
      *(long *)target = reversebytes(*quadword);
#else
      //    *(long *) target = *quadword;
      str_cpy_all(target, (char *)&doublesource, length);
#endif
      break;
    }

    case REC_BYTE_F_ASCII: {
      if (CollationInfo::isSystemCollation(collation)) {
        Int16 nPasses = CollationInfo::getCollationNPasses(collation);

        if (collType == CollationInfo::Sort || collType == CollationInfo::Compare) {
          encodeCollationKey((const UInt8 *)source, length, (UInt8 *)target, tgtLength, nPasses, collation, TRUE);
        } else  // search
        {
          int effEncodedKeyLength = 0;
          encodeCollationSearchKey((const UInt8 *)source, length, (UInt8 *)target, tgtLength, effEncodedKeyLength,
                                   nPasses, collation, TRUE);
          assert(tgtAttr && tgt_varlen_ptr);
          tgtAttr->setVarLength(effEncodedKeyLength, tgt_varlen_ptr);
        }
      } else {
        //------------------------------------------
        if (target != source) str_cpy_all(target, source, length);

        if (isCaseInsensitive) {
          // upcase target
          for (int i = 0; i < length; i++) {
            target[i] = TOUPPER(source[i]);
          }
        }
        //--------------------------
      }

    } break;

    case REC_BYTE_V_ASCII:
    case REC_BYTE_V_ASCII_LONG: {
      int vc_len = attr->getLength(varlenPtr);

      if (CollationInfo::isSystemCollation(collation)) {
        Int16 nPasses = CollationInfo::getCollationNPasses(collation);
        NABoolean rmTspaces = getRmTSpaces(collation);

        if (collType == CollationInfo::Sort || collType == CollationInfo::Compare) {
          encodeCollationKey((UInt8 *)source, (Int16)vc_len, (UInt8 *)target, tgtLength, nPasses, collation, rmTspaces);
        } else {
          int effEncodedKeyLength = 0;
          encodeCollationSearchKey((UInt8 *)source, (Int16)vc_len, (UInt8 *)target, tgtLength, effEncodedKeyLength,
                                   nPasses, collation, rmTspaces);

          assert(tgtAttr && tgt_varlen_ptr);
          tgtAttr->setVarLength(effEncodedKeyLength, tgt_varlen_ptr);
        }
      } else {
        //
        // Copy the source to the target.
        //
        if (!isCaseInsensitive)
          str_cpy_all(target, source, vc_len);
        else {
          // upcase target
          for (int i = 0; i < vc_len; i++) {
            target[i] = TOUPPER(source[i]);
          }
        }

        //
        // Blankpad the target (if needed).
        //
        if (vc_len < length) str_pad(&target[vc_len], (int)(length - vc_len), ' ');
      }

    } break;

    // added for Unicode data type.
    case REC_NCHAR_V_UNICODE: {
      int vc_len = attr->getLength(varlenPtr);

      //
      // Copy the source to the target.
      //
      str_cpy_all(target, source, vc_len);

      //
      // Blankpad the target (if needed).
      //
      if (vc_len < length)
        wc_str_pad((NAWchar *)&target[vc_len], (int)(length - vc_len) / sizeof(NAWchar),
                   unicode_char_set::space_char());

#if defined(NA_LITTLE_ENDIAN)
      wc_swap_bytes((NAWchar *)target, length / sizeof(NAWchar));
#endif
      break;
    }

    // added for Unicode data type.
    case REC_NCHAR_F_UNICODE: {
      if (target != source) str_cpy_all(target, source, length);

#if defined(NA_LITTLE_ENDIAN)
      wc_swap_bytes((NAWchar *)target, length / sizeof(NAWchar));
#endif

      break;
    }

    case REC_BYTE_V_ANSI: {
      short vc_len;
      vc_len = strlen(source);

      //
      // Copy the source to the target.
      //
      str_cpy_all(target, source, vc_len);

      //
      // Blankpad the target (if needed).
      //
      if (vc_len < length) str_pad(&target[vc_len], (int)(length - vc_len), ' ');
    } break;

    default:
      //
      // Encoding is not needed.  Just copy the source to the target.
      //
      if (target != source) str_cpy_all(target, source, length);
      break;
  }
}

////////////////////////////////////////////////////////////////////
// class ex_function_encode
////////////////////////////////////////////////////////////////////
ex_function_encode::ex_function_encode(){};
ex_function_encode::ex_function_encode(OperatorTypeEnum oper_type, Attributes **attr, Space *space, short descFlag)
    : ex_function_clause(oper_type, 2, attr, space), flags_(0), collation_((Int16)CharInfo::DefaultCollation) {
  if (descFlag)
    setIsDesc(TRUE);
  else
    setIsDesc(FALSE);

  setCollEncodingType(CollationInfo::Sort);
};
ex_function_encode::ex_function_encode(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                       CharInfo::Collation collation, short descFlag,
                                       CollationInfo::CollationType collType)
    : ex_function_clause(oper_type, 2, attr, space), flags_(0), collation_((Int16)collation) {
  if (descFlag)
    setIsDesc(TRUE);
  else
    setIsDesc(FALSE);

  setCollEncodingType(collType);
};

ex_expr::exp_return_type ex_function_encode::processNulls(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  if ((CollationInfo::isSystemCollation((CharInfo::Collation)collation_)) &&
      getCollEncodingType() != CollationInfo::Sort) {
    return ex_clause::processNulls(op_data, heap, diagsArea);
  } else if (regularNullability()) {
    return ex_clause::processNulls(op_data, heap, diagsArea);
  }

  // if value is missing,
  // then move max or min value to result.
  if (getOperand(1)->getNullFlag() && (!op_data[1]))  // missing value (is a null value)
  {
    if (NOT isDesc()) {
      // NULLs sort high for ascending comparison.
      // Pad result with highest value.

      // For SQL/MP tables, DP2 expects missing value columns to be
      // 0 padded after the null-indicator.
      str_pad(op_data[2 * MAX_OPERANDS], (int)getOperand(0)->getStorageLength(), '\0');
      str_pad(op_data[2 * MAX_OPERANDS], ExpTupleDesc::KEY_NULL_INDICATOR_LENGTH, '\377');
    } else {
      // NULLs sort low for descending comparison.
      // Pad result with lowest value.
      str_pad(op_data[2 * MAX_OPERANDS], (int)getOperand(0)->getStorageLength(), '\377');
      str_pad(op_data[2 * MAX_OPERANDS], ExpTupleDesc::KEY_NULL_INDICATOR_LENGTH, '\0');
    }
    return ex_expr::EXPR_NULL;
  }

  return ex_expr::EXPR_OK;
};

ex_expr::exp_return_type ex_function_encode::evalDecode(char *op_data[], CollHeap *heap) {
  char *result = op_data[0];
  Attributes *srcOp = getOperand(1);

  decodeKeyValue(srcOp, isDesc(), op_data[1], op_data[-MAX_OPERANDS + 1], result, op_data[-MAX_OPERANDS], FALSE);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_encode::eval(char *op_data[], CollHeap *heap, ComDiagsArea **) {
  if (isDecode()) {
    return evalDecode(op_data, heap);
  }

  Int16 prependedLength = 0;
  char *result = op_data[0];
  Attributes *tgtOp = getOperand(0);
  Attributes *srcOp = getOperand(1);

  if ((srcOp->getNullFlag()) &&  // nullable
      (NOT regularNullability())) {
    // If target is aligned format then can't use the 2 byte null here ...
    assert(!tgtOp->isSQLMXAlignedFormat());

    // if sort is set for char types with collations (including default)
    if (getCollEncodingType() == CollationInfo::Sort) {
      // value cannot be null in this proc. That is handled in process_nulls.
      str_pad(result, ExpTupleDesc::KEY_NULL_INDICATOR_LENGTH, '\0');
      result += ExpTupleDesc::KEY_NULL_INDICATOR_LENGTH;
      prependedLength = ExpTupleDesc::KEY_NULL_INDICATOR_LENGTH;
    }
  }

  if (srcOp->isComplexType())
    ((ComplexType *)srcOp)->encode(op_data[1], result, isDesc());
  else {
    int tgtLength = tgtOp->getLength() - prependedLength;
    encodeKeyValue(srcOp, op_data[1], op_data[-MAX_OPERANDS + 1], result, caseInsensitive(), tgtOp,
                   op_data[-MAX_OPERANDS], tgtLength, (CharInfo::Collation)collation_, getCollEncodingType());
  }

  if (isDesc()) {
    // compliment all bytes
    for (int k = 0; k < tgtOp->getLength(); k++) op_data[0][k] = (char)(~(op_data[0][k]));
  }

  return ex_expr::EXPR_OK;
}

void ex_function_encode::getCollationWeight(CharInfo::Collation collation, Int16 pass, UInt16 chr, UInt8 *weightStr,
                                            Int16 &weightStrLen) {
  UChar wght = getCollationWeight(collation, pass, chr);
  switch (collation) {
    case CharInfo::CZECH_COLLATION:
    case CharInfo::CZECH_COLLATION_CI: {
      if ((CollationInfo::Pass)pass != CollationInfo::SecondPass) {
        if (wght > 0) {
          weightStr[0] = wght;
          weightStrLen = 1;
        } else {
          weightStrLen = 0;
        }
      } else {
        if (getCollationWeight(collation, CollationInfo::FirstPass, chr) > 0) {
          weightStr[0] = wght;
          weightStrLen = 1;
        } else {
          weightStr[0] = 0;
          weightStr[1] = wght;
          weightStrLen = 2;
        }
      }
    } break;
    default: {
      if (wght > 0) {
        weightStr[0] = wght;
        weightStrLen = 1;
      } else {
        weightStrLen = 0;
      }
    }
  }
}
unsigned char ex_function_encode::getCollationWeight(CharInfo::Collation collation, Int16 pass, UInt16 chr) {
  return collParams[CollationInfo::getCollationParamsIndex(collation)].weightTable[pass][chr];
}

Int16 ex_function_encode::getNumberOfDigraphs(const CharInfo::Collation collation) {
  return collParams[CollationInfo::getCollationParamsIndex(collation)].numberOfDigraphs;
}

UInt8 *ex_function_encode::getDigraph(const CharInfo::Collation collation, const int digraphNum) {
  return (UInt8 *)collParams[CollationInfo::getCollationParamsIndex(collation)].digraphs[digraphNum];
}

Int16 ex_function_encode::getDigraphIndex(const CharInfo::Collation collation, const int digraphNum) {
  return collParams[CollationInfo::getCollationParamsIndex(collation)].digraphIdx[digraphNum];
}

NABoolean ex_function_encode::getRmTSpaces(const CharInfo::Collation collation) {
  return collParams[CollationInfo::getCollationParamsIndex(collation)].rmTSpaces;
}

NABoolean ex_function_encode::getNumberOfChars(const CharInfo::Collation collation) {
  return collParams[CollationInfo::getCollationParamsIndex(collation)].numberOfChars;
}

NABoolean ex_function_encode::isOneToOneCollation(const CharInfo::Collation collation) {
  for (UInt16 i = 0; i < getNumberOfChars(collation); i++) {
    for (UInt16 j = i + 1; j < getNumberOfChars(collation); j++) {
      NABoolean isOneToOne = FALSE;
      for (Int16 pass = 0; pass < CollationInfo::getCollationNPasses(collation); pass++) {
        if (getCollationWeight(collation, pass, i) != getCollationWeight(collation, pass, j)) {
          isOneToOne = TRUE;
        }
      }
      if (!isOneToOne) {
        return FALSE;
      }
    }
  }
  return TRUE;
}

void ex_function_encode::encodeCollationKey(const UInt8 *src, int srcLength, UInt8 *encodeKey,
                                            const int encodedKeyLength, Int16 nPasses, CharInfo::Collation collation,
                                            NABoolean rmTSpaces) {
  assert(CollationInfo::isSystemCollation(collation));

  UInt8 *ptr;

  if (src[0] == CollationInfo::getCollationMaxChar(collation)) {
    str_pad((char *)encodeKey, srcLength, CollationInfo::getCollationMaxChar(collation));
    if (str_cmp((char *)src, (char *)encodeKey, srcLength) == 0) {
      str_pad((char *)encodeKey, encodedKeyLength, '\377');
      return;
    }
  }

  if (src[0] == '\0') {
    str_pad((char *)encodeKey, encodedKeyLength, '\0');

    if (str_cmp((char *)src, (char *)encodeKey, srcLength) == 0) {
      return;
    }
  }

  Int16 charNum = 0;
  NABoolean hasDigraphs = FALSE;
  int trailingSpaceLength = 0;
  UInt8 digraph[2];
  digraph[0] = digraph[1] = 0;

  Int16 weightStrLength = 0;
  ptr = encodeKey;

  /////////////////////////////////////////////

  for (int i = srcLength - 1; rmTSpaces && i > 0 && src[i] == 0x20; i--) {
    trailingSpaceLength++;
  }

  for (short i = CollationInfo::FirstPass; i < nPasses; i++) {
    if (i != CollationInfo::FirstPass) {
      *ptr++ = 0x0;
    }

    if ((i == CollationInfo::FirstPass) || (i != CollationInfo::FirstPass && hasDigraphs)) {
      // loop through the chars in the string, find digraphs an assighn weights
      for (int srcIdx = 0; srcIdx < srcLength - trailingSpaceLength; srcIdx++) {
        digraph[0] = digraph[1];
        digraph[1] = src[srcIdx];
        NABoolean digraphFound = FALSE;
        for (int j = 0; j < getNumberOfDigraphs(collation); j++) {
          if (digraph[0] == getDigraph(collation, j)[0] && digraph[1] == getDigraph(collation, j)[1]) {
            digraphFound = hasDigraphs = TRUE;
            charNum = getDigraphIndex(collation, j);
            ptr--;
            break;
          }
        }
        if (!digraphFound) {
          charNum = src[srcIdx];
        }
        getCollationWeight(collation, i, charNum, ptr, weightStrLength);
        ptr = ptr + weightStrLength;
      }
    } else {
      for (int srcIdx = 0; srcIdx < srcLength - trailingSpaceLength; srcIdx++) {
        charNum = src[srcIdx];
        getCollationWeight(collation, i, charNum, ptr, weightStrLength);
        ptr = ptr + weightStrLength;
      }
    }
  }

  str_pad((char *)ptr, (encodeKey - ptr) + encodedKeyLength, '\0');

}  // ex_function_encode::encodeCollationKey

void ex_function_encode::encodeCollationSearchKey(const UInt8 *src, int srcLength, UInt8 *encodeKey,
                                                  const int encodedKeyLength, int &effEncodedKeyLength, Int16 nPasses,
                                                  CharInfo::Collation collation, NABoolean rmTSpaces) {
  assert(CollationInfo::isSystemCollation(collation));

  UInt8 *ptr;

  Int16 charNum = 0;
  NABoolean hasDigraphs = FALSE;
  int trailingSpaceLength = 0;
  UInt8 digraph[2];
  digraph[0] = digraph[1] = 0;

  ptr = encodeKey;

  /////////////////////////////////////////////
  for (int i = srcLength - 1; rmTSpaces && i > 0 && src[i] == 0x20; i--) {
    trailingSpaceLength++;
  }

  for (int srcIdx = 0; srcIdx < srcLength - trailingSpaceLength; srcIdx++) {
    digraph[0] = digraph[1];
    digraph[1] = src[srcIdx];
    NABoolean digraphFound = FALSE;
    for (int j = 0; j < getNumberOfDigraphs(collation); j++) {
      if (digraph[0] == getDigraph(collation, j)[0] && digraph[1] == getDigraph(collation, j)[1]) {
        digraphFound = hasDigraphs = TRUE;
        charNum = getDigraphIndex(collation, j);
        ptr = ptr - nPasses;
        break;
      }
    }
    if (!digraphFound) {
      charNum = src[srcIdx];
    }

    // don't include ignorable characters
    short ignorable = 0;
    for (short np = 0; np < nPasses; np++) {
      ptr[np] = getCollationWeight(collation, np, charNum);

      if (ptr[np] == '\0') {
        ignorable++;
      }
    }
    if (ignorable != nPasses)  //
    {
      ptr = ptr + nPasses;
    }

    if (digraphFound && ignorable != nPasses) {
      for (short np = CollationInfo::FirstPass; np < nPasses; np++) {
        ptr[np] = '\0';
      }
      ptr = ptr + nPasses;
    }
  }

  effEncodedKeyLength = ptr - encodeKey;

  str_pad((char *)ptr, (encodeKey - ptr) + encodedKeyLength, '\0');

}  // ex_function_encode::encodeCollationSearchKey

////////////////////////////////////////////////////////////////////////
// class ex_function_explode_varchar
////////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ex_function_explode_varchar::processNulls(char *op_data[], CollHeap *heap,
                                                                   ComDiagsArea **diagsArea) {
  Attributes *tgt = getOperand(0);

  if (getOperand(1)->getNullFlag() && (!op_data[1]))  // missing value (is a null value)
  {
    if (tgt->getNullFlag())  // if result is nullable
    {
      // move null value to result
      ExpTupleDesc::setNullValue(op_data[0], tgt->getNullBitIndex(), tgt->getTupleFormat());

      if (forInsert_) {
        // move 0 to length bytes
        tgt->setVarLength(0, op_data[MAX_OPERANDS]);
      }  // for Insert
      else {
        // move maxLength to result length bytes
        tgt->setVarLength(tgt->getLength(), op_data[MAX_OPERANDS]);
      }
      return ex_expr::EXPR_NULL;  // indicate that a null input was processed
    } else {
      // Attempt to put NULL into column with NOT NULL NONDROPPABLE constraint.
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_ASSIGNING_NULL_TO_NOT_NULL, derivedFunction(),
                              origFunctionOperType());

      return ex_expr::EXPR_ERROR;
    }
  }  // source is a null value

  // first operand is not null -- set null indicator in result if needed
  if (tgt->getNullFlag()) {
    ExpTupleDesc::clearNullValue(op_data[0], tgt->getNullBitIndex(), tgt->getTupleFormat());
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_explode_varchar::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  if (forInsert_) {
    // move source to target. No blankpadding.
    return convDoIt(op_data[1], getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]), getOperand(1)->getDatatype(),
                    getOperand(1)->getPrecision(), getOperand(1)->getScale(), op_data[0], getOperand(0)->getLength(),
                    getOperand(0)->getDatatype(), getOperand(0)->getPrecision(), getOperand(0)->getScale(),
                    op_data[-MAX_OPERANDS], getOperand(0)->getVCIndicatorLength(), heap, diagsArea);
  } else {
    // move source to target. Blankpad target to maxLength.
    if (convDoIt(op_data[1], getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]), getOperand(0)->getDatatype(),
                 getOperand(1)->getPrecision(), getOperand(1)->getScale(), op_data[0], getOperand(0)->getLength(),
                 REC_BYTE_F_ASCII, getOperand(0)->getPrecision(), getOperand(0)->getScale(), NULL, 0, heap, diagsArea))
      return ex_expr::EXPR_ERROR;

    // Move max length to length bytes of target.
    getOperand(0)->setVarLength(getOperand(0)->getLength(), op_data[-MAX_OPERANDS]);
  }

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
// class ex_function_hash
////////////////////////////////////////////////////////////////////
int ex_function_hash::HashHash(int inValue) {
  // Hashhash -
  // input :   inValue  -  double word to be hashed
  // output :  30-bit hash values uniformly distributed (mod s) for
  //           any s < 2**30

  // This algorithm creates near-uniform output for arbitrarily distributed
  // input by selecting for each fw of the key a quasi-random universal
  // hash function from the class of linear functions ax + b (mod p)
  // over the field of integers modulo the prime 2**31-1. The output is at
  // least comparable in quality to cubics of the form
  // ax**3 + bx**2 + cx  + d (mod p), and is considerably closer to true
  // uniformity than a single linear function chosen once per execution.
  // The latter preserve the uniform 2nd central moment of bucket totals,
  // and the former the 4th central moment. For probabilistic counting
  // applications, the theoretical standard error cannot be achieved with
  // less than cubic polynomials, but the present algorithm is approx 3-5x
  // in speed. (Cf. histogram doc. for bibliography, but especially:
  //    Carter and Wegman, "Universal Clases of Hash Functions",
  //       Journ. Comp. Sys. Sci., 18: April 1979, pp. 145-154
  //       22: 1981, pp. 265-279
  //    Dietzfelbinger, et al., "Polynomial Hash Functions...",
  //       ICALP '92, pp. 235-246. )

  // N.B. - For modular arithmetic the 64-bit product of two 32-bit
  // operands must be reduced (mod p). The high-order 32 bits are available
  // in hardware but not necessarily through C syntax.

  // Two additional optimizations should be noted:
  // 1. Instead of processing 3-byte operands, as would be required with
  //    universal hashing over the field 2**31-1, with alignment delays, we
  //    process fullwords, and choose distinct 'random' coefficients for
  //    2 keys congruent (mod p) using a 32-bit function, and then proceed
  //    with modular linear hashing over the smaller field.
  // 2. For p = 2**c -1 for any c, shifts, and's and or's can be substituted
  //    for division, as recommended by Carter and Wegman. In addition, the
  //    output distribution is unaffected (i.e. with probability
  //    < 1/(2**31-1) if we omit tests for 0 (mod p).
  //    To reduce a (mod p), create k1 and k2 (<= p) with a = (2**31)k1 + k2,
  //    and reduce again to (2**31)k3 + k4, where k4 < 2**31 and k3 = 0 or 1.

  // Multi-word keys:
  // If k = k1||...||kn we compute the quasi-random coefficients c & d using
  // ki, but take h(ki) = c*(ki xor h(ki-1)) + d, where  h(k0) = 0, and  use
  // H(k) = h(kn). This precludes the commutative anomaly
  // H(k || k') = H(k' || k)

  register int u, v, c, d, k0;
  int a1, a2, b1, b2;

  int c1 = (int)5233452345LL;
  int c2 = (int)8578458478LL;
  int d1 = 1862598173LL;
  int d2 = 3542657857LL;

  int hashValue = 0;

  int k = inValue;

  u = (c1 >> 16) * (k >> 16);
  v = c1 * k;
  c = u ^ v ^ c2;
  u = (d1 >> 16) * (k >> 16);
  v = d1 * k;
  d = u ^ v ^ d2;

  c = ((c & 0x80000000) >> 31) + (c & 0x7fffffff);
  d = ((d & 0x80000000) >> 31) + (d & 0x7fffffff);

  /* compute hash value 1  */

  k0 = hashValue ^ k;

  /*hmul(c,k0);
    u=u0; v=v0;*/

  a1 = c >> 16;
  a2 = c & 0xffff;
  b1 = k0 >> 16;
  b2 = k0 & 0xffff;

  v = (((a1 * b2) & 0xffff) + ((b1 * a2) & 0xffff));
  u = a1 * b1 + (((a1 * b2) >> 16) + ((b1 * a2) >> 16)) + ((v & 0x10000) >> 16);

  v = c * k0;
  if (v < (a2 * b2)) u++;

  u = u << 1;
  u = ((v & 0x80000000) >> 31) | u;
  v = v & 0x7fffffff;
  v = u + v;
  v = ((v & 0x80000000) >> 31) + (v & 0x7fffffff);
  /*v = ((v & 0x80000000) >> 31) + (v & 0x7fffffff);
    if ( v == 0x7fffffff) v = 0;*/

  v = v + d;
  v = ((v & 0x80000000) >> 31) + (v & 0x7fffffff);
  /*v = ((v & 0x80000000) >> 31) + (v & 0x7fffffff);
    if ( v == 0x7fffffff) v = 0;*/

  return (v);
};

ex_expr::exp_return_type ex_function_hash::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  Attributes *srcOp = getOperand(1);
  int hashValue = 0;

  if (srcOp->getNullFlag() && (!op_data[-(2 * MAX_OPERANDS) + 1])) {
    // operand is a null value. All null values hash to
    // the same hash value. Choose any arbitrary constant
    // number as the hash value.
    hashValue = ExHDPHash::nullHashValue;  //;666654765;
  } else {
    // get the actual length stored in the data, or fixed length
    int length = srcOp->getLength(op_data[-MAX_OPERANDS + 1]);

    // if VARCHAR, skip trailing blanks and adjust length.
    if (srcOp->getVCIndicatorLength() > 0) {
      switch (srcOp->getDatatype()) {
          // added to correctly handle VARNCHAR.
        case REC_NCHAR_V_UNICODE: {
          // skip trailing blanks
          NAWchar *wstr = (NAWchar *)(op_data[1]);
          int wstr_length = length / sizeof(NAWchar);

          while ((wstr_length > 0) && (wstr[wstr_length - 1] == unicode_char_set::space_char())) wstr_length--;

          length = sizeof(NAWchar) * wstr_length;
        } break;

        default:
          // case  REC_BYTE_V_ASCII:

          // skip trailing blanks
          while ((length > 0) && (op_data[1][length - 1] == ' ')) length--;
          break;
      }
    }

    UInt32 flags = ExHDPHash::NO_FLAGS;

    switch (srcOp->getDatatype()) {
      case REC_NCHAR_V_UNICODE:
      case REC_NCHAR_V_ANSI_UNICODE:
        flags = ExHDPHash::SWAP_TWO;
        break;
    }

    hashValue = ExHDPHash::hash(op_data[1], flags, length);
  };

  *(int *)op_data[0] = hashValue;

  return ex_expr::EXPR_OK;
};

int ex_function_hivehash::hashForCharType(char *data, int length) {
  // To compute: SUM (i from 0 to n-1) (s(i) * 31^(n-1-i)

  int hash = 1;
  int tempHash = 0;
  for (long i = 0; i < length; i++) {
    // perform result * 31, optimized as (result <<5 - result)
    tempHash = hash;
    hash <<= 5;
    hash -= tempHash;
    hash += (int)data[i];
  }

  return hash;
}

int ex_function_hivehash::getLocalTimeZone() {
  time_t time_utc;
  struct tm tm_local;
  struct tm tm_gmt;
  // Get the UTC time
  time(&time_utc);
  // Get the local time
  localtime_r(&time_utc, &tm_local);
  // Change it to GMT tm
  gmtime_r(&time_utc, &tm_gmt);

  int time_zone = (int)(tm_local.tm_hour - tm_gmt.tm_hour);
  if (time_zone < -12) {
    time_zone += 24;
  } else if (time_zone > 12) {
    time_zone -= 24;
  }
  return time_zone;
}

int ex_function_hivehash::getDateSeconds(char *data, bool calcSecond) {
  char *prt;
  UInt16 month;
  UInt16 day;
  UInt16 year;

  UInt16 hour;
  UInt16 min;
  UInt16 second;

  prt = data;
  year = (UInt16)(*(UInt16 *)prt);
  prt = data + 2;
  month = (UInt16)(*(UInt8 *)prt);
  prt = data + 3;
  day = (UInt16)(*(UInt8 *)prt);

  struct tm tm_date;
  time_t seconds;
  tm_date.tm_year = year - 1900;
  tm_date.tm_mon = month - 1;
  tm_date.tm_mday = day;

  if (!calcSecond) {
    hour = 0;
    min = 0;
    second = 0;
  } else {
    prt = data + 4;
    hour = (UInt16)(*(UInt8 *)prt);
    prt = data + 5;
    min = (UInt16)(*(UInt8 *)prt);
    prt = data + 6;
    second = (UInt16)(*(UInt8 *)prt);
  }

  tm_date.tm_hour = hour;
  tm_date.tm_min = min;
  tm_date.tm_sec = second;

  seconds = mktime(&tm_date);
  return seconds;
}

int ex_function_hivehash::hashForDateType(char *data, int length) {
  int seconds = getDateSeconds(data, false);

  int time_zone = getLocalTimeZone();
  int timediff = time_zone * 3600;
  seconds += timediff;

  int days;
  if (seconds >= 0) {
    days = (int)(seconds / 86400);
  } else {
    days = (int)((seconds - 86399) / 86400);
  }
  return days;
}

int ex_function_hivehash::getMicroseconds(char *data, int length) {
  assert(length != 7 && length != 11);
  int microseconds = 0;
  if (length == 7) {
    return 0;
  } else if (length == 11) {
    char *ptr;
    ptr = data + 7;
    microseconds = (int)(*(int *)ptr);
  }

  return microseconds;
}

int ex_function_hivehash::hashForTimestampType(char *data, int length) {
  long seconds = (long)getDateSeconds(data, true);
  int micsec = getMicroseconds(data, length);
  seconds <<= 30;
  seconds |= micsec;
  return (int)((seconds >> 32 & 0x00000000ffffffff) ^ seconds);
}

// for INTEGER    (4 bytes)
//    SMALLINT   (2 bytes)
//    TINYINT    (1 byte)
//    all return same value as hashCode
int ex_function_hivehash::hashForIntType(int *data) { return *data; }

int ex_function_hivehash::hashForLargeIntType(long *a) {
  unsigned long b = (unsigned long)*a;
  return (int)((b >> 32) ^ *a);
}

int ex_function_hivehash::hashForFloatType(float *v) {
  int ret = 0;
  memcpy(&ret, v, sizeof(v));
  return ret;
}

int ex_function_hivehash::hashForDoubleType(double *v) {
  long lv = 0;
  int ret = 0;
  memcpy(&lv, v, sizeof(v));
  return hashForLargeIntType(&lv);
}

ex_expr::exp_return_type ex_function_hivehash::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  Attributes *srcOp = getOperand(1);
  int hashValue = 0;
  int length;

  if (srcOp->getNullFlag() && (!op_data[-(2 * MAX_OPERANDS) + 1])) {
    // operand is a null value. All null values hash to the same hash value.
    hashValue = 0;  // hive semantics: hash(NULL) = 0
  } else if ((DFS2REC::isAnyVarChar(srcOp->getDatatype())) && getOperand(1)->getVCIndicatorLength() > 0) {
    length = srcOp->getLength(op_data[-MAX_OPERANDS + 1]);
    hashValue = ex_function_hivehash::hashForCharType(op_data[1], length);
  } else if (DFS2REC::isSQLFixedChar(srcOp->getDatatype())) {
    length = srcOp->getLength();
    hashValue = ex_function_hivehash::hashForCharType(op_data[1], length);
  } else if (DFS2REC::isSQLDateTime(srcOp->getDatatype())) {
    length = srcOp->getLength();
    if (srcOp->getPrecision() == REC_DTCODE_DATE) {
      hashValue = ex_function_hivehash::hashForDateType(op_data[1], length);
    }
    if (srcOp->getPrecision() == REC_DTCODE_TIMESTAMP) {
      hashValue = ex_function_hivehash::hashForTimestampType(op_data[1], length);
    }
  } else if (srcOp->getDatatype() == REC_BIN32_SIGNED) {
    length = srcOp->getLength();
    hashValue = ex_function_hivehash::hashForIntType((int *)op_data[1]);
  } else if (srcOp->getDatatype() == REC_BIN64_SIGNED) {
    length = srcOp->getLength();
    hashValue = ex_function_hivehash::hashForLargeIntType((long *)op_data[1]);
  } else if (srcOp->getDatatype() == REC_IEEE_FLOAT32) {
    length = srcOp->getLength();
    hashValue = ex_function_hivehash::hashForFloatType((float *)op_data[1]);
  } else if (srcOp->getDatatype() == REC_IEEE_FLOAT64) {
    length = srcOp->getLength();
    hashValue = ex_function_hivehash::hashForDoubleType((double *)op_data[1]);
  } else if (DFS2REC::isBinaryNumeric(srcOp->getDatatype())) {
    hashValue = *(int *)(op_data[1]);
  }  // TBD: other SQ types

  printf("Hive HashCode is [%d]\n", hashValue);

  *(int *)op_data[0] = hashValue;
  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
// class ExHashComb
////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ExHashComb::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // always assume that both operands and result are of the same
  // (unsigned) type and length

  // with built-in long long type we could also support 8 byte integers
  int op1, op2;

  switch (getOperand(0)->getStorageLength()) {
    case 4:
      op1 = *((int *)op_data[1]);
      op2 = *((int *)op_data[2]);
      *((int *)op_data[0]) = ((op1 << 1) | (op1 >> 31)) ^ op2;
      break;
    default:
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

      return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
// class ExHiveHashComb
////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ExHiveHashComb::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // always assume that both operands and result are of the same
  // (signed) type and length

  // with built-in long long type we could also support 8 byte integers
  int op1, op2;

  switch (getOperand(0)->getStorageLength()) {
    case 4:
      op1 = *((int *)op_data[1]);
      op2 = *((int *)op_data[2]);

      // compute op1 * 31 + op2, optimized as op1 << 5 - op1 + op2
      *((int *)op_data[0]) = op1 << 5 - op1 + op2;
      break;

    default:
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

      return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

// -------------------------------------------------------------
// Hash Functions used by Hash Partitioning. These functions cannot
// change once Hash Partitioning is released!  Defined for all data
// types, returns a 32 bit non-nullable hash value for the data item.
// The ::hash() function uses a loop over the key bytes; the other
// hash2()/hash4()/hash8() are more efficient but are only applicable
// to keys whose sizes are known at compile time: 2/4/8 bytes.
//
// Any modification to this method that would compute different hash
// rsult should take a look at BloomFilter::computeFinalHash()
// where an optimization is in place. The same compuatation is also
// repeated in BloomFilterEval class in the embedded bloom filter
// for Arrow reader.
//--------------------------------------------------------------

int ExHDPHash::hash(const char *data, UInt32 flags, int length) {
  int hashValue = 0;
  unsigned char *valp = (unsigned char *)data;
  int iter = 0;  // iterator over the key bytes, if needed

  switch (flags) {
    case NO_FLAGS:
    case SWAP_EIGHT: {
      // Speedup for long keys - compute first 8 bytes fast (the rest with a loop)
      if (length >= 8) {
        hashValue = hash8(data, flags);  // do the first 8 bytes fast
        // continue with the 9-th byte (only when length > 8 )
        valp = (unsigned char *)&data[8];
        iter = 8;
      }

      for (; iter < length; iter++) {
        // Make sure the hashValue is sensitive to the byte position.
        // One bit circular shift.

        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
      }
      break;
    }
    case SWAP_TWO: {
      // Speedup for long keys - compute first 8 bytes fast (the rest with a loop)
      if (length >= 8) {
        hashValue = hash8(data, flags);  // do the first 8 bytes fast
        // continue with the 9-th byte (only when length > 8 )
        valp = (unsigned char *)&data[8];
        iter = 8;
      }

      // Loop over all the bytes of the value and compute the hash value.
      for (; iter < length; iter += 2) {
        // Make sure the hashValue is sensitive to the byte position.
        // One bit circular shift.
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 1)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp];
        valp += 2;
      }
      break;
    }
    case SWAP_FOUR: {
      hashValue = hash4(data, flags);
      break;
    }
    case (SWAP_FIRSTTWO | SWAP_LASTFOUR):
    case SWAP_FIRSTTWO:
    case SWAP_LASTFOUR: {
      if ((flags & SWAP_FIRSTTWO) != 0) {
        hashValue = randomHashValues[*(valp + 1)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp];
        valp += 2;
        iter += 2;
      }

      if ((flags & SWAP_LASTFOUR) != 0) {
        length -= 4;
      }

      for (; iter < length; iter++) {
        // Make sure the hashValue is sensitive to the byte position.
        // One bit circular shift.
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*valp++];
      }

      if ((flags & SWAP_LASTFOUR) != 0) {
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 3)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 2)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 1)];
        hashValue = (hashValue << 1 | hashValue >> 31) ^ randomHashValues[*(valp + 0)];
      }
      break;
    }
    default:
      assert(FALSE);
  }

  return hashValue;
}

ex_expr::exp_return_type ExHDPHash::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  Attributes *srcOp = getOperand(1);
  int hashValue;

  if (srcOp->getNullFlag() && (!op_data[-(2 * MAX_OPERANDS) + 1])) {
    // operand is a null value. All null values hash to
    // the same hash value. Choose any arbitrary constant
    // number as the hash value.
    //
    hashValue = ExHDPHash::nullHashValue;  // 666654765;
  } else {
    int length = (int)srcOp->getLength(op_data[-MAX_OPERANDS + 1]);

    // if VARCHAR, skip trailing blanks and adjust length.
    if (srcOp->getVCIndicatorLength() > 0) {
      switch (srcOp->getDatatype()) {
          // added to correctly handle VARNCHAR.
        case REC_NCHAR_V_UNICODE: {
          // skip trailing blanks
          NAWchar *wstr = (NAWchar *)(op_data[1]);
          int wstr_length = length / sizeof(NAWchar);

          while ((wstr_length > 0) && (wstr[wstr_length - 1] == unicode_char_set::space_char())) wstr_length--;

          length = sizeof(NAWchar) * wstr_length;
        } break;
        default:

          // skip trailing blanks
          while ((length > 0) && (op_data[1][length - 1] == ' ')) length--;
          break;
      }
    }

    UInt32 flags = NO_FLAGS;

    switch (srcOp->getDatatype()) {
      case REC_NUM_BIG_UNSIGNED:
      case REC_NUM_BIG_SIGNED:
      case REC_BIN16_SIGNED:
      case REC_BIN16_UNSIGNED:
      case REC_NCHAR_F_UNICODE:
      case REC_NCHAR_V_UNICODE:
      case REC_NCHAR_V_ANSI_UNICODE:
        flags = SWAP_TWO;
        break;
      case REC_BIN32_SIGNED:
      case REC_BIN32_UNSIGNED:
      case REC_IEEE_FLOAT32:
        flags = SWAP_FOUR;
        break;
      case REC_BIN64_SIGNED:
      case REC_BIN64_UNSIGNED:
      case REC_IEEE_FLOAT64:
        flags = SWAP_EIGHT;
        break;
      case REC_DATETIME: {
        rec_datetime_field start;
        rec_datetime_field end;
        ExpDatetime *datetime = (ExpDatetime *)srcOp;
        datetime->getDatetimeFields(srcOp->getPrecision(), start, end);
        if (start == REC_DATE_YEAR) {
          flags = SWAP_FIRSTTWO;
        }
        if (end == REC_DATE_SECOND && srcOp->getScale() > 0) {
          flags |= SWAP_LASTFOUR;
        }

      } break;
      default:
        if (srcOp->getDatatype() >= REC_MIN_INTERVAL && srcOp->getDatatype() <= REC_MAX_INTERVAL) {
          if (srcOp->getLength() == 8)
            flags = SWAP_EIGHT;
          else if (srcOp->getLength() == 4)
            flags = SWAP_FOUR;
          else if (srcOp->getLength() == 2)
            flags = SWAP_TWO;
          else
            assert(FALSE);
        }
    }

    hashValue = hash(op_data[1], flags, length);
  }

  *(int *)op_data[0] = hashValue;

  return ex_expr::EXPR_OK;
}  // ExHDPHash::eval()

// --------------------------------------------------------------
// This function is used to combine two hash values to produce a new
// hash value. Used by Hash Partitioning. This function cannot change
// once Hash Partitioning is released!  Defined for all data types,
// returns a 32 bit non-nullable hash value for the data item.
// --------------------------------------------------------------
ex_expr::exp_return_type ExHDPHashComb::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // always assume that both operands and result are of the same
  // (unsigned) type and length

  assert(getOperand(0)->getStorageLength() == 4 && getOperand(1)->getStorageLength() == 4 &&
         getOperand(2)->getStorageLength() == 4);

  int op1, op2;

  op1 = *((int *)op_data[1]);
  op2 = *((int *)op_data[2]);

  // One bit, circular shift
  op1 = ((op1 << 1) | (op1 >> 31));

  op1 = op1 ^ op2;

  *((int *)op_data[0]) = op1;

  return ex_expr::EXPR_OK;
}  // ExHDPHashComb::eval()

// ex_function_replace_null
//
ex_expr::exp_return_type ex_function_replace_null::processNulls(char *op_data[], CollHeap *heap,
                                                                ComDiagsArea **diagsArea) {
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_replace_null::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  Attributes *tgt = getOperand(0);

  // Mark the result as non-null
  if (tgt->getNullFlag())
    ExpTupleDesc::clearNullValue(op_data[-(2 * MAX_OPERANDS)], tgt->getNullBitIndex(), tgt->getTupleFormat());

  // If the input is NULL, replace it with the value in op_data[3]
  if (!op_data[-(2 * MAX_OPERANDS) + 1]) {
    for (int i = 0; i < tgt->getStorageLength(); i++) op_data[0][i] = op_data[3][i];
  } else {
    for (int i = 0; i < tgt->getStorageLength(); i++) op_data[0][i] = op_data[2][i];
  }

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
// class ex_function_mod
////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ex_function_mod::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int lenr = (int)getOperand(0)->getLength();
  int len1 = (int)getOperand(1)->getLength();
  int len2 = (int)getOperand(2)->getLength();

  long op1, op2, result, commonScale;
  int scale1, scale2;

  switch (len1) {
    case 1:
      op1 = *((Int8 *)op_data[1]);
      break;
    case 2:
      op1 = *((short *)op_data[1]);
      break;
    case 4:
      op1 = *((int *)op_data[1]);
      break;
    case 8:
      op1 = *((long *)op_data[1]);
      break;
    default:
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());

      return ex_expr::EXPR_ERROR;
  }

  switch (len2) {
    case 1:
      op2 = *((Int8 *)op_data[2]);
      break;
    case 2:
      op2 = *((short *)op_data[2]);
      break;
    case 4:
      op2 = *((int *)op_data[2]);
      break;
    case 8:
      op2 = *((long *)op_data[2]);
      break;
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }

  if (op2 == 0) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_DIVISION_BY_ZERO, derivedFunction(), origFunctionOperType());
    return ex_expr::EXPR_ERROR;
  }

  scale1 = (int)getOperand(1)->getScale();
  scale2 = (int)getOperand(2)->getScale();

  // We calculate the remainder by first scaling the two numbers with the larger of the two scales, and then apply "%"
  // like this:
  //   MOD(5.233, 1.2) = 5233 % 1200 = 433
  // the return type is numeric(4,3), so we can get
  //   MOD(5.233, 1.2) = 0.433

  if (scale1 <= scale2) {
    commonScale = pow(10, (scale2 - scale1));
    op1 *= commonScale;
  } else {
    commonScale = pow(10, (scale1 - scale2));
    op2 *= commonScale;
  }

  result = op1 % op2;

  switch (lenr) {
    case 1:
      *((Int8 *)op_data[0]) = (short)result;
      break;
    case 2:
      *((short *)op_data[0]) = (short)result;
      break;
    case 4:
      *((int *)op_data[0]) = (int)result;
      break;
    case 8:
      *((long *)op_data[0]) = result;
      break;
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
// class ex_function_mask
////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ex_function_mask::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // always assume that both operands and result are of the same
  // (unsigned) type and length

  // with built-in long long type we could also support 8 byte integers
  int op1, op2, result;

  switch (getOperand(0)->getStorageLength()) {
    case 1:
      op1 = *((UInt8 *)op_data[1]);
      op2 = *((UInt8 *)op_data[2]);
      if (getOperType() == ITM_MASK_SET) {
        result = op1 | op2;
      } else {
        result = op1 & ~op2;
      }
      *((unsigned short *)op_data[0]) = (unsigned short)result;
      break;
    case 2:
      op1 = *((unsigned short *)op_data[1]);
      op2 = *((unsigned short *)op_data[2]);
      if (getOperType() == ITM_MASK_SET) {
        result = op1 | op2;
      } else {
        result = op1 & ~op2;
      }
      *((unsigned short *)op_data[0]) = (unsigned short)result;
      break;
    case 4:
      op1 = *((int *)op_data[1]);
      op2 = *((int *)op_data[2]);
      if (getOperType() == ITM_MASK_SET) {
        result = op1 | op2;
      } else {
        result = op1 & ~op2;
      }
      *((int *)op_data[0]) = result;
      break;
    case 8: {
      long lop1 = *((long *)op_data[1]);
      long lop2 = *((long *)op_data[2]);
      long lresult;
      if (getOperType() == ITM_MASK_SET) {
        lresult = lop1 | lop2;
      } else {
        lresult = lop1 & ~lop2;
      }
      *((long *)op_data[0]) = lresult;
      break;
    }
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
// class ExFunctionShift
////////////////////////////////////////////////////////////////////
ex_expr::exp_return_type ExFunctionShift::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  if (getOperand(2)->getStorageLength() != 4) {
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  int shift = *((int *)op_data[2]);
  int value, result;

  switch (getOperand(0)->getStorageLength()) {
    case 1:
      value = *((UInt8 *)op_data[1]);
      if (getOperType() == ITM_SHIFT_RIGHT) {
        result = value >> shift;
      } else {
        result = value << shift;
      }
      *((UInt8 *)op_data[0]) = (UInt8)result;
      break;
    case 2:
      value = *((unsigned short *)op_data[1]);
      if (getOperType() == ITM_SHIFT_RIGHT) {
        result = value >> shift;
      } else {
        result = value << shift;
      }
      *((unsigned short *)op_data[0]) = (unsigned short)result;
      break;
    case 4:
      value = *((int *)op_data[1]);
      if (getOperType() == ITM_SHIFT_RIGHT) {
        result = value >> shift;
      } else {
        result = value << shift;
      }
      *((int *)op_data[0]) = result;
      break;
    case 8: {
      long value = *((long *)op_data[1]);
      long result;
      if (getOperType() == ITM_SHIFT_RIGHT) {
        result = value >> shift;
      } else {
        result = value << shift;
      }
      *((long *)op_data[0]) = result;
      break;
    }
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}
static ex_expr::exp_return_type getDoubleValue(double *dest, char *source, Attributes *operand, CollHeap *heap,
                                               ComDiagsArea **diagsArea) {
  switch (operand->getDatatype()) {
    case REC_FLOAT64:
      *dest = *(double *)(source);
      return ex_expr::EXPR_OK;
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }
}

static ex_expr::exp_return_type setDoubleValue(char *dest, Attributes *operand, double *source, CollHeap *heap,
                                               ComDiagsArea **diagsArea) {
  switch (operand->getDatatype()) {
    case REC_FLOAT64:
      *(double *)dest = *source;
      return ex_expr::EXPR_OK;
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
      return ex_expr::EXPR_ERROR;
  }
}

ex_expr::exp_return_type ExFunctionSVariance::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  double sumOfValSquared = 0;
  double sumOfVal = 0;
  double countOfVal = 1;
  double avgOfVal;
  double result = 0;

  if (getDoubleValue(&sumOfValSquared, op_data[1], getOperand(1), heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  if (getDoubleValue(&sumOfVal, op_data[2], getOperand(2), heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  if (getDoubleValue(&countOfVal, op_data[3], getOperand(3), heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  avgOfVal = sumOfVal / countOfVal;

  if (countOfVal == 1 && getOperType() == ITM_VARIANCE_SAMP) {
    result = 0.0;
  } else {
    switch (getOperType()) {
      case ITM_VARIANCE_SAMP:
        result = (sumOfValSquared - (sumOfVal * avgOfVal)) / (countOfVal - 1);
        break;
      case ITM_VARIANCE_POP:
        result = (sumOfValSquared - (sumOfVal * avgOfVal)) / (countOfVal);
        break;
    }
    if (result < 0.0) {
      result = 0.0;
    }
  }

  if (setDoubleValue(op_data[0], getOperand(0), &result, heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionSStddev::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  double sumOfValSquared = 0;
  double sumOfVal = 0;
  double countOfVal = 1;
  double avgOfVal;
  double result = 0;

  if (getDoubleValue(&sumOfValSquared, op_data[1], getOperand(1), heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  if (getDoubleValue(&sumOfVal, op_data[2], getOperand(2), heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  if (getDoubleValue(&countOfVal, op_data[3], getOperand(3), heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  avgOfVal = sumOfVal / countOfVal;

  if (countOfVal == 1 && getOperType() == ITM_STDDEV_SAMP) {
    result = 0.0;
  } else {
    short err = 0;
    switch (getOperType()) {
      case ITM_STDDEV_SAMP:
        result = (sumOfValSquared - (sumOfVal * avgOfVal)) / (countOfVal - 1);
        break;
      case ITM_STDDEV_POP:
        result = (sumOfValSquared - (sumOfVal * avgOfVal)) / (countOfVal);
        break;
    }
    if (result < 0.0) {
      result = 0.0;
    } else {
      result = MathSqrt(result, err);
    }

    if (err) {
      ExRaiseSqlError(heap, diagsArea, EXE_BAD_ARG_TO_MATH_FUNC);
      **diagsArea << DgString0("SQRT");

      ExRaiseSqlError(heap, diagsArea, EXE_MAPPED_FUNCTION_ERROR);
      **diagsArea << DgString0("STDDEV");

      return ex_expr::EXPR_ERROR;
    }
  }

  if (setDoubleValue(op_data[0], getOperand(0), &result, heap, diagsArea)) {
    return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpRaiseErrorFunction::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char catName[ComAnsiNamePart::MAX_IDENTIFIER_EXT_LEN + 1];
  char schemaName[ComAnsiNamePart::MAX_IDENTIFIER_EXT_LEN + 1];

  // Don't do anything with the op[] data
  // Create a DiagsArea to return the SQLCODE and the ConstraintName
  // and TableName.
  if (raiseError())
    ExRaiseSqlError(heap, diagsArea, (ExeErrorCode)getSQLCODE(), NULL, NULL, NULL, NULL, getOptionalStr());
  else
    ExRaiseSqlWarning(heap, diagsArea, (ExeErrorCode)getSQLCODE(), NULL, NULL, NULL, NULL, getOptionalStr());

  // SQLCODE correspoding to Triggered Action Exception
  if (getSQLCODE() == ComDiags_TrigActionExceptionSQLCODE) {
    assert(constraintName_ && tableName_);

    extractCatSchemaNames(catName, schemaName, constraintName_);

    *(*diagsArea) << DgTriggerCatalog(catName);
    *(*diagsArea) << DgTriggerSchema(schemaName);
    *(*diagsArea) << DgTriggerName(constraintName_);

    extractCatSchemaNames(catName, schemaName, tableName_);

    *(*diagsArea) << DgCatalogName(catName);
    *(*diagsArea) << DgSchemaName(schemaName);
    *(*diagsArea) << DgTableName(tableName_);
  } else if (getSQLCODE() == ComDiags_SignalSQLCODE)  // Signal Statement
  {
    if (constraintName_) *(*diagsArea) << DgString0(constraintName_);  // The SQLSTATE

    if (getNumOperands() == 2) {
      int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
      op_data[1][len1] = '\0';
      *(*diagsArea) << DgString1(op_data[1]);  // The string expression
    } else if (tableName_)
      *(*diagsArea) << DgString1(tableName_);  // The message
  } else {
    if (constraintName_) {
      extractCatSchemaNames(catName, schemaName, constraintName_);

      *(*diagsArea) << DgConstraintCatalog(catName);
      *(*diagsArea) << DgConstraintSchema(schemaName);
      *(*diagsArea) << DgConstraintName(constraintName_);
    }
    if (tableName_) {
      extractCatSchemaNames(catName, schemaName, tableName_);

      *(*diagsArea) << DgCatalogName(catName);
      *(*diagsArea) << DgSchemaName(schemaName);
      *(*diagsArea) << DgTableName(tableName_);
    }
  }

  // If it's a warning, we should return a predictable boolean value.
  *((int *)op_data[0]) = 0;

  if (raiseError())
    return ex_expr::EXPR_ERROR;
  else
    return ex_expr::EXPR_OK;
}

// -----------------------------------------------------------------------
// methods for ExFunctionPack
// -----------------------------------------------------------------------
// Constructor.
ExFunctionPack::ExFunctionPack(Attributes **attr, Space *space, int width, int base, NABoolean nullsPresent)
    : ex_function_clause(ITM_PACK_FUNC, 3, attr, space), width_(width), base_(base) {
  setNullsPresent(nullsPresent);
}

// Evaluator.
ex_expr::exp_return_type ExFunctionPack::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char guard1 = op_data[0][-1];
  char guard2 = op_data[0][getOperand(0)->getLength()];

  // Extract no of rows already in the packed record.
  int noOfRows;
  str_cpy_all((char *)&noOfRows, op_data[0], sizeof(int));

  // Extract the packing factor.
  int pf = *(int *)op_data[2];

  // The clause returns an error for no more slots in the packed record.
  if (noOfRows >= pf) {
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  // Whether the source is null.
  char *nullFlag = op_data[-2 * ex_clause::MAX_OPERANDS + 1];

  // If null bit map is present in the packed record.
  if (nullsPresent()) {
    // Offset of null bit from the beginning of the null bitmap.
    int nullBitOffsetInBytes = noOfRows >> 3;

    // Offset of null bit from the beginning of the byte it is in.
    int nullBitOffsetInBits = noOfRows & 0x7;

    // Extract the byte in which the null bit is in.
    char *nullByte = op_data[0] + nullBitOffsetInBytes + sizeof(int);

    // Used to set/unset the null bit.
    unsigned char nullByteMask = (1 << nullBitOffsetInBits);

    // Turn bit off/on depending on whether operand is null.
    if (nullFlag == 0)
      *nullByte |= nullByteMask;  // set null bit on.
    else
      *nullByte &= (~nullByteMask);  // set null bit off.
  } else if (nullFlag == 0) {
    // Bit map is not present but input is null. We got a problem.
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  // We have contents to copy only if source is not null.
  if (nullFlag != 0) {
    // Width of each packet in the packed record. -ve means in no of bits.
    if (width_ < 0) {
      int widthInBits = -width_;

      // Length of data region which has already been occupied in bits.
      int tgtBitsOccupied = (noOfRows * widthInBits);

      // Byte offset for data of this packet from beginning of data region.
      int tgtByteOffset = base_ + (tgtBitsOccupied >> 3);

      // Bit offset for data of this packet from beginning of its byte.
      int tgtBitOffset = (tgtBitsOccupied & 0x7);

      // Byte offset of data source left to be copied.
      int srcByteOffset = 0;

      // Bit offset of data source from beginning of its byte to be copied.
      int srcBitOffset = 0;

      // No of bits to copy in total.
      int bitsToCopy = widthInBits;

      // There are still bits remaining to be copied.
      while (bitsToCopy > 0) {
        // Pointer to the target byte.
        char *tgtBytePtr = (op_data[0] + tgtByteOffset);

        // No of bits left in the target byte.
        int bitsLeftInTgtByte = 8 - tgtBitOffset;

        // No of bits left in the source byte.
        int bitsLeftInSrcByte = 8 - srcBitOffset;

        int bitsToCopyThisRound = (bitsLeftInTgtByte > bitsLeftInSrcByte ? bitsLeftInSrcByte : bitsLeftInTgtByte);

        if (bitsToCopyThisRound > bitsToCopy) bitsToCopyThisRound = bitsToCopy;

        // Mask has ones in the those positions where bits will be copied to.
        unsigned char mask =
            ((0xFF >> tgtBitOffset) << (8 - bitsToCopyThisRound)) >> (8 - tgtBitOffset - bitsToCopyThisRound);

        // Clear target bits. Keep other bits unchanged in the target byte.
        (*tgtBytePtr) &= (~mask);

        // Align source bits with its the destination. Mask off other bits.
        unsigned char srcByte = *(op_data[1] + srcByteOffset);
        srcByte = ((srcByte >> srcBitOffset) << tgtBitOffset) & mask;

        // Make the copy.
        (*tgtBytePtr) |= srcByte;

        // Move source byte and bit offsets.
        srcBitOffset += bitsToCopyThisRound;
        if (srcBitOffset >= 8) {
          srcByteOffset++;
          srcBitOffset -= 8;
        }

        // Move target byte and bit offsets.
        tgtBitOffset += bitsToCopyThisRound;
        if (tgtBitOffset >= 8) {
          tgtByteOffset++;
          tgtBitOffset -= 8;
        }

        bitsToCopy -= bitsToCopyThisRound;
      }
    } else  // width_ > 0
    {
      // Width in bytes: we can copy full strings of bytes.
      int tgtByteOffset = base_ + (noOfRows * width_);
      str_cpy_all(op_data[0] + tgtByteOffset, op_data[1], width_);
    }
  }

  // Update the "noOfRows" in the packed record.
  noOfRows++;
  str_cpy_all(op_data[0], (char *)&noOfRows, sizeof(int));

  // $$$ supported as a CHAR rather than a VARCHAR for now.
  // getOperand(0)->
  // setVarLength(offset+lengthToCopy,op_data[-ex_clause::MAX_OPERANDS]);

  if (guard1 != op_data[0][-1] || guard2 != op_data[0][getOperand(0)->getLength()]) {
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  // Signal a completely packed record to the caller.
  if (noOfRows == pf) return ex_expr::EXPR_TRUE;

  // Signal an incompletely packed record to the caller.
  return ex_expr::EXPR_FALSE;
}

// ExUnPackCol::eval() ------------------------------------------
// The ExUnPackCol clause extracts a set of bits from a CHAR value.
// The set of  bits to extract is described by a base offset, a width,
// and an index.  The offset and width are known at compile time, but
// the index is a run time variable.  ExUnPackCol clause also gets
// the null indicator of the result from a bitmap within the CHAR
// field.
//
ex_expr::exp_return_type ExUnPackCol::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // The width of the extract in BITS.
  //
  int width = width_;

  // The base offset of the data in BYTES.
  //
  int base = base_;

  // Boolean indicating if the NULL Bitmap is present.
  // If it is present, then it starts at a 4 (sizeof(int)) byte offset.
  //
  NABoolean np = nullsPresent();

  // Which piece of data are we extracting.
  //
  int index = *(int *)op_data[2];

  // NULL Processing...
  //
  if (np) {
    // The bit to be extracted.
    //
    int bitOffset = index;

    // The byte of the CHAR field containing the bit.
    //
    int byteOffset = sizeof(int) + (bitOffset >> 3);

    // The bit of the byte at byteOffset to be extracted.
    //
    bitOffset = bitOffset & 0x7;

    // A pointer to the null indicators of the operands.
    //
    char **null_data = &op_data[-2 * ex_clause::MAX_OPERANDS];

    // The mask used to test the NULL bit.
    //
    UInt32 mask = 1 << bitOffset;

    // The byte containing the NULL Flag.
    //
    UInt32 byte = op_data[1][byteOffset];

    // Is the NULL Bit set?
    //
    if (byte & mask) {
      // The value is NULL, so set the result to NULL, and
      // return since we do not need to extract the data.
      //
      *(short *)null_data[0] = (short)0xFFFF;
      return ex_expr::EXPR_OK;
    } else {
      // The value is non-NULL, so set the indicator,
      // continue to extract the data value.
      //
      *(short *)null_data[0] = 0;
    }
  }

  // Bytes masks used for widths (1-8) of bit extracts.
  //
  const UInt32 masks[] = {0, 1, 3, 7, 15, 31, 63, 127, 255};

  // Handle some special cases:
  // Otherwise do a generic bit extract.
  //
  if (width == 8 || width == 4 || width == 2 || width == 1) {
    // Items per byte for special case widths (1-8).
    //
    const UInt32 itemsPerBytes[] = {0, 8, 4, 2, 2, 1, 1, 1, 1};

    // Amount to shift the index to get a byte index for the
    // special case widths.
    //
    const UInt32 itemsPerByteShift[] = {0, 3, 2, 1, 1, 0, 0, 0, 0};

    // Extracted value.
    //
    UInt32 value;

    // An even more special case.
    //
    if (width == 8) {
      // Must use unsigned assignment so that sign extension is not done.
      // Later when signed bit precision integers are support will have
      // to have a special case for those.
      //
      value = (unsigned char)op_data[1][base + index];
    } else {
      // The number of items in a byte.
      //
      UInt32 itemsPerByte = itemsPerBytes[width];

      // The amount to shift the index to get a byte offset.
      //
      UInt32 shift = itemsPerByteShift[width];

      // The offset of the byte containing the value.
      //
      int byteIndex = index >> shift;

      // The index into the byte of the value.
      //
      int itemIndex = index & (itemsPerByte - 1);

      // A mask to extract an item of size width.
      //
      UInt32 mask = masks[width];

      // The byte containing the item.
      //
      value = op_data[1][base + byteIndex];

      // Shift the byte, so that the value to be
      // extracted is in the least significant bits.
      //
      value = value >> (width * itemIndex);

      // Clear all bits except those of the value.
      //
      value = value & mask;
    }

    // Copy value to result.
    //
    switch (getOperand(0)->getLength()) {
      case 1:
        *(unsigned char *)op_data[0] = value;
        return ex_expr::EXPR_OK;
      case 2:
        *(unsigned short *)op_data[0] = value;
        return ex_expr::EXPR_OK;
      case 4:
        *(int *)op_data[0] = value;
        return ex_expr::EXPR_OK;
      default:
        // ERROR - This should never happen.
        //
        ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
        return ex_expr::EXPR_ERROR;
    }

    return ex_expr::EXPR_OK;
  }

  // Handle special case of a Byte copy.
  //
  if ((width % 8) == 0) {
    width = width / 8;

    str_cpy_all(op_data[0], &op_data[1][base + (index * width)], width);
    return ex_expr::EXPR_OK;
  }

  char guard1 = op_data[0][-1];
  char guard2 = op_data[0][getOperand(0)->getLength()];

  // The general case of arbitrary bit lengths that can span byte boundaries.
  //

  // The offset to the value in bits.
  //
  int bitOffset = index * width;

  // The offset to the last bit of the value in bits.
  //
  int bitOffsetEnd = bitOffset + width - 1;

  // The offset to the byte containing the first bit of the value.
  // in bytes.
  //
  int byteOffset = base + (bitOffset >> 3);

  // The offset to the byte containing the first bit beyond the value.
  // in bytes.
  //
  int byteOffsetEnd = base + (bitOffsetEnd >> 3);

  // The offset of the first bit in the byte.
  //
  bitOffset = bitOffset & 0x7;

  // The amount to shift the byte to the right to align
  // the lower portion.
  //
  int rshift = bitOffset;

  // The amount to shift the byte to the left to align
  // the upper portion.
  //
  int lshift = 8 - bitOffset;

  // An index into the destination.
  //
  int dindex = 0;

  // Copy all the bits to the destination.
  //
  int i = byteOffset;
  for (; i <= byteOffsetEnd; i++) {
    // Get a byte containing bits of the value.
    //
    unsigned char byte = op_data[1][i];

    if (dindex > 0) {
      // After the first byte, must copy the upper
      // portion of the byte to the previous byte of
      // the result. This is the second time writing
      // to this byte.
      //
      op_data[0][dindex - 1] |= byte << lshift;
    }

    if (dindex < (int)getOperand(0)->getLength()) {
      // Copy the lower portion of this byte of the result
      // to the destination.  This is the first time this
      // byte is written to.
      //
      op_data[0][dindex] = byte >> rshift;
    }

    dindex++;
  }

  // Clear all bits of the result that did not come
  // from the extracted value.
  //
  for (i = 0; i < (int)getOperand(0)->getLength(); i++) {
    unsigned char mask = (width > 7) ? 0xFF : masks[width];

    op_data[0][i] &= mask;
    width -= 8;
    width = (width < 0) ? 0 : width;
  }

  if (guard1 != op_data[0][-1] || guard2 != op_data[0][getOperand(0)->getLength()]) {
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return ex_expr::EXPR_ERROR;
  }

  return ex_expr::EXPR_OK;
}
ex_expr::exp_return_type ex_function_translate::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int copyLen = 0;
  int convertedLen = 0;
  int convType = get_conv_type();

  Attributes *op0 = getOperand(0);
  Attributes *op1 = getOperand(1);
  int convFlags = (flags_ & TRANSLATE_FLAG_ALLOW_INVALID_CODEPOINT ? CONV_ALLOW_INVALID_CODE_VALUE : 0);

  return convDoIt(op_data[1], op1->getLength(op_data[-MAX_OPERANDS + 1]), op1->getDatatype(), op1->getPrecision(),
                  (convType == CONV_UTF8_F_UCS2_V) ? (int)(CharInfo::UTF8) : op1->getScale(), op_data[0],
                  op0->getLength(), op0->getDatatype(), op0->getPrecision(),
                  (convType == CONV_UCS2_F_UTF8_V) ? (int)(CharInfo::UTF8) : op0->getScale(), op_data[-MAX_OPERANDS],
                  op0->getVCIndicatorLength(), heap, diagsArea, (ConvInstruction)convType, NULL, convFlags);
}

void ExFunctionRandomNum::initSeed(char *op_data[]) {
  if (seed_ <= 0) {
    QRLogger::log(CAT_SQL_EX_SPLIT_BOTTOM, LL_DEBUG, "ExFunctionRandomNum::initSeed(): seed_=%d, simpleRandom()=%d\n",
                  seed_, simpleRandom());

    if (simpleRandom()) {
      if (seed_ == -1) {
        // initialize with the thread id
        // (same logic is in ex_expr::evalPCode())
        const char *fixSeed = getenv("FIXED_ROUND_ROBIN_SEED");
        if (!fixSeed)
          seed_ = 1031 * (((int)GETTID()) % 1031);
        else {
          seed_ = getExeGlobals()->getMyInstanceNumber();  // 0-based
        }

        seed_++;
      } else
        // start with 1 and go up to max
        seed_ = 1;

      QRLogger::log(CAT_SQL_EX_SPLIT_BOTTOM, LL_DEBUG, "ExFunctionRandomNum::initSeed(): updated seed_=%d\n", seed_);

      return;
    }

    assert(seed_ == 0);

    if (getNumOperands() == 2) {
      // seed is specified as an argument. Use it.
      seed_ = *(int *)op_data[1];
      return;
    }

    // Pick an initial seed.  According to the reference given below
    // (in the eval function), all initial seeds between 1 and
    // 2147483647 are equally valid.  So, we just need to pick one
    // in this range.  Do this based on a timestamp.
    struct timespec seedTime;

    clock_gettime(CLOCK_REALTIME, &seedTime);

    seed_ = (int)(seedTime.tv_sec % 2147483648);
    seed_ ^= (int)(seedTime.tv_nsec % 2147483648L);

    // Go through one step of a linear congruential random generator.
    // (https://en.wikipedia.org/wiki/Linear_congruential_generator).
    // This is to avoid seed values that are close to each other when
    // we call this method again within a short time. The eval() method
    // below doesn't handle seed values that are close to each other
    // very well.
    seed_ = (((long)seed_) * 1664525L + 1013904223L) % 2147483648;

    if (seed_ < 0) seed_ += 2147483647;
    if (seed_ < 1) seed_ = 1;
  }
}

void ExFunctionRandomNum::genRand(char *op_data[]) {
  // Initialize seed if not already done
  initSeed(op_data);

  int t = 0;
  const int M = 2147483647;
  if (simpleRandom()) {
    t = seed_ + 1;
  } else {
    // Algorithm is taken from "Random Number Generators: Good Ones
    // Are Hard To Find", by Stephen K. Park and Keith W. Miller,
    // Communications of the ACM, Volume 31, Number 10, Oct 1988.

    const int A = 16807;
    const int Q = 127773;
    const int R = 2836;

    int h = seed_ / Q;
    int l = seed_ % Q;
    t = A * l - R * h;
  }

  if (t > 0)
    seed_ = t;
  else
    seed_ = t + M;
}

ex_expr::exp_return_type ExFunctionRandomNum::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  genRand(op_data);  // generates and sets the random number in seed_

  *((int *)op_data[0]) = (int)seed_;

  return ex_expr::EXPR_OK;
}

void ExFunctionRandomSelection::initDiff() {
  if (difference_ == -1) {
    difference_ = 0;

    while (selProbability_ >= 1.0) {
      difference_++;
      selProbability_ -= 1.0;
    }

    // Normalize the selProbability to a 32 bit integer and store in
    // normProbability

    normProbability_ = (int)(selProbability_ * 0x7fffffff);

    // reset the selProbability_ to original value in case this function
    // gets called again

    selProbability_ += difference_;
  }
}

ex_expr::exp_return_type ExFunctionRandomSelection::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  initDiff();  // gets executed only once

  genRand(NULL);  // generates and sets the random number in seed_

  if (getRand() < normProbability_)
    *((int *)op_data[0]) = (int)(difference_ + 1);
  else
    *((int *)op_data[0]) = (int)(difference_);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExHash2Distrib::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  int keyValue = *(int *)op_data[1];
  int numParts = *(int *)op_data[2];
  int partNo = (int)(((long)keyValue * (long)numParts) >> 32);

  *(int *)op_data[0] = partNo;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExProgDistrib::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  int keyValue = *(int *)op_data[1];
  int totNumValues = *(int *)op_data[2];
  int resultValue = 1;
  int offset = keyValue;
  int i = 2;

  while (offset >= i && i <= totNumValues) {
    int n1 = offset % i;
    int n2 = offset / i;
    if (n1 == 0) {
      offset = (i - 1) * (n2 - 1) + resultValue;
      resultValue = i;
      i++;
    } else {
      int n3 = n2 << 1;

      if (n1 > n3) {
        int n = n1 / n3 + (n1 % n3 != 0);
        offset -= n2 * n;
        i += n;
      } else {
        offset -= n2;
        i++;
      }
    }
  }

  *((int *)op_data[0]) = resultValue - 1;
  return ex_expr::EXPR_OK;
}
ex_expr::exp_return_type ExProgDistribKey::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  int value = *(int *)op_data[1];
  int offset = *(int *)op_data[2];
  int totNumValues = *(int *)op_data[3];
  int uniqueVal = offset >> 16;
  offset = offset & 0x0000FFFF;

  value++;

  int i = totNumValues;
  while (i >= 2) {
    if (value == i) {
      value = (int)(offset - 1) % (i - 1) + 1;
      offset = ((offset - 1) / (i - 1) + 1) * i;
      i--;
    } else if (offset < i) {
      i = (offset > value ? offset : value);
    } else {
      offset = offset + (offset - 1) / (i - 1);
      i--;
    }
  }

  long result = offset;
  result = ((result << 16) | uniqueVal) << 16;

  *((long *)op_data[0]) = result;

  return ex_expr::EXPR_OK;
}
ex_expr::exp_return_type ExPAGroup::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  int partNum = *(int *)op_data[1];
  int totNumGroups = *(int *)op_data[2];
  int totNumParts = *(int *)op_data[3];

  int scaleFactor = totNumParts / totNumGroups;
  int transPoint = (totNumParts % totNumGroups);

  int groupPart;

  if (partNum < (transPoint * (scaleFactor + 1))) {
    groupPart = partNum / (scaleFactor + 1);
  } else {
    groupPart = (partNum - transPoint) / scaleFactor;
  }

  *((int *)op_data[0]) = groupPart;
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionRangeLookup::eval(char *op_data[], CollHeap *, ComDiagsArea **) {
  // Two operands get passed to ExFunctionRangeLookup: a pointer to
  // the actual, encoded key, and a pointer into a constant array
  // that contains the encoded split ranges. The result is a 4 byte
  // integer, not NULL, that contains the partition number.
  char *encodedKey = op_data[1];
  char *sKeys = op_data[2];
  int *result = (int *)op_data[0];

  // Now perform a binary search in sKeys

  int lo = 0;
  int hi = numParts_;  // note we have one more entry than parts
  int probe;
  int cresult;

  while (hi - lo > 1) {
    // try the element in the middle (may round down)
    probe = (lo + hi) / 2;

    // compare our encoded key with that middle split range
    cresult = str_cmp(encodedKey, &sKeys[probe * partKeyLen_], partKeyLen_);
    if (cresult <= 0) hi = probe;  // search first half, discard second half

    if (cresult >= 0) lo = probe;  // search second half, discard first half
  }

  // Once we have narrowed it down to a difference between lo and hi
  // of 0 or 1, we know that lo points to the index of our partition
  // because the partition number must be greater or equal to lo and
  // less than hi. Remember that we set hi to one more than we had
  // partition numbers.
  *result = lo;

  return ex_expr::EXPR_OK;
}

ExRowsetArrayScan::ExRowsetArrayScan(){};
ExRowsetArrayRowid::ExRowsetArrayRowid(){};
ExRowsetArrayInto::ExRowsetArrayInto(){};

ExRowsetArrayScan::ExRowsetArrayScan(Attributes **attr, Space *space, int maxNumElem, int elemSize,
                                     NABoolean elemNullInd, NABoolean optLargVar)
    : maxNumElem_(maxNumElem),
      elemSize_(elemSize),
      elemNullInd_(elemNullInd),
      optLargVar_(optLargVar),
      ex_function_clause(ITM_ROWSETARRAY_SCAN, 3, attr, space){};

ExRowsetArrayRowid::ExRowsetArrayRowid(Attributes **attr, Space *space, int maxNumElem)
    : maxNumElem_(maxNumElem), ex_function_clause(ITM_ROWSETARRAY_ROWID, 3, attr, space){};

ExRowsetArrayInto::ExRowsetArrayInto(Attributes **attr, Space *space, int maxNumElem, int elemSize,
                                     NABoolean elemNullInd)
    : maxNumElem_(maxNumElem),
      numElem_(0),
      elemSize_(elemSize),
      elemNullInd_(elemNullInd),
      ex_function_clause(ITM_ROWSETARRAY_INTO, 3, attr, space)

          {};

// ExRowsetArrayScan::eval() ------------------------------------------
// The ExRowsetArrayScan clause extracts an element of the Rowset array
// The size of the element is known at compile time, but the index is a
// run time variable.
ex_expr::exp_return_type ExRowsetArrayScan::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // op_data[0] points to the result
  // op_data[1] points to the array
  // op_data[2] points to the index

  int index = *(int *)op_data[2];

  if (index < 0 || index >= maxNumElem_) {
    // The index cannot be greater than the dimension of the array
    // It is likely that there was an item expression evaluated at
    // execution time to obtain the rowsetSize which is greater than
    // the maximum allowed.
    ExRaiseSqlError(heap, diagsArea, EXE_ROWSET_INDEX_OUTOF_RANGE);
    **diagsArea << DgSqlCode(-EXE_ROWSET_INDEX_OUTOF_RANGE);

    return ex_expr::EXPR_ERROR;
  }

  Attributes *ResultAttr = getOperand(0);
  Attributes *SourceAttr = getOperand(1);
  int size = ResultAttr->getStorageLength();
  char *SourceElemPtr = &op_data[1][(index * size) + sizeof(int)];

  // NULL Processing...
  if (elemNullInd_) {
    // A pointer to the null indicators of the operands.
    char **ResultNullData = &op_data[-2 * ex_clause::MAX_OPERANDS];
    char *SourceElemIndPtr = SourceElemPtr;

    SourceElemPtr += SourceAttr->getNullIndicatorLength();

    // Set the indicator
    if (ResultAttr->getNullFlag()) {
      str_cpy_all(ResultNullData[0], SourceElemIndPtr, SourceAttr->getNullIndicatorLength());
    }

    if (ExpTupleDesc::isNullValue(SourceElemIndPtr, SourceAttr->getNullBitIndex(), SourceAttr->getTupleFormat())) {
      // The value is NULL, return since we do not need to extract the data.
      return ex_expr::EXPR_NULL;
    }
  }
  long vcLen = 0;

  // For SQLVarChars, we have to copy both length and value fields.
  // op_data[-ex_clause::MAX_OPERANDS] points to the length field of the
  // SQLVarChar;
  // The size of the field is sizeof(short) for rowset SQLVarChars.
  if (SourceAttr->getVCIndicatorLength() > 0) {
    str_cpy_all((char *)op_data[-ex_clause::MAX_OPERANDS],
                (char *)(&op_data[-ex_clause::MAX_OPERANDS + 1][index * size]),
                SourceAttr->getVCIndicatorLength());  // sizeof(short));
    if (optLargVar_) {
      if (SourceAttr->getVCIndicatorLength() == sizeof(short))
        vcLen = *(short *)op_data[-ex_clause::MAX_OPERANDS];
      else
        vcLen = *(int *)op_data[-ex_clause::MAX_OPERANDS];
    } else {
      vcLen = ResultAttr->getLength();
    }
    SourceElemPtr += SourceAttr->getVCIndicatorLength();
    str_cpy_all(op_data[0], SourceElemPtr, vcLen);
  } else {
    // Note we do not have variable length for host variables. But we may not
    // need to copy the whole length for strings.
    str_cpy_all(op_data[0], SourceElemPtr, ResultAttr->getLength());
  }

  return ex_expr::EXPR_OK;
}

Long ExRowsetArrayScan::pack(void *space) { return packClause(space, sizeof(ExRowsetArrayScan)); }

Long ExRowsetArrayRowid::pack(void *space) { return packClause(space, sizeof(ExRowsetArrayRowid)); }

Long ExRowsetArrayInto::pack(void *space) { return packClause(space, sizeof(ExRowsetArrayInto)); }

// ExRowsetArrayRowid::eval() ------------------------------------------
// The ExRowsetArrayRowid clause returns the value of the current index
ex_expr::exp_return_type ExRowsetArrayRowid::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // op_data[0] points to the result
  // op_data[1] points to the array
  // op_data[2] points to the index

  // The width of each data item in bytes

  int index = *(int *)op_data[2];

  if (index < 0 || index >= maxNumElem_) {
    // The index cannot be greater than the dimension of the array
    // It is likely that there was an item expression evaluated at
    // execution time to obtain the rowsetSize which is greater than
    // the maximum allowed.
    ExRaiseSqlError(heap, diagsArea, EXE_ROWSET_INDEX_OUTOF_RANGE);
    **diagsArea << DgSqlCode(-EXE_ROWSET_INDEX_OUTOF_RANGE);
    return ex_expr::EXPR_ERROR;
  }

  // Note we do not have variable length for host variables. But we may not
  // need to copy the whole length for strings.
  str_cpy_all(op_data[0], (char *)&index, sizeof(index));

  return ex_expr::EXPR_OK;
}

// ExRowsetArrayInto::eval() ------------------------------------------
// The ExRowsetArrayInto clause appends a value into the Rowset array
// The size of the element is known at compile time
ex_expr::exp_return_type ExRowsetArrayInto::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // op_data[0] points to the array (Result)
  // op_data[1] points to the value to insert
  // op_data[2] points to the rowset size expression

  int runtimeMaxNumElem = *(int *)op_data[2];

  if (numElem_ >= runtimeMaxNumElem || numElem_ >= maxNumElem_) {
    // Overflow, we cannot add more elements to this rowset array
    ExRaiseSqlError(heap, diagsArea, EXE_ROWSET_OVERFLOW);
    **diagsArea << DgSqlCode(-EXE_ROWSET_OVERFLOW);
    return ex_expr::EXPR_ERROR;
  }

  // Get number of rows stored in the array
  int nrows;
  str_cpy_all((char *)&nrows, op_data[0], sizeof(int));

  if (nrows >= runtimeMaxNumElem || nrows >= maxNumElem_) {
    // Overflow, we cannot add more elements to this rowset array
    ExRaiseSqlError(heap, diagsArea, EXE_ROWSET_OVERFLOW);
    **diagsArea << DgSqlCode(-EXE_ROWSET_OVERFLOW);
    return ex_expr::EXPR_ERROR;
  }

  Attributes *resultAttr = getOperand(0);
  NABoolean resultIsNull = FALSE;
  char *sourceNullData = op_data[-2 * ex_clause::MAX_OPERANDS + 1];
  Attributes *sourceAttr = getOperand(1);

  int elementSize = ((SimpleType *)resultAttr)->getStorageLength();

  char *resultElemPtr = &op_data[0][(nrows * elementSize) + sizeof(int)];

  // NULL Processing...
  if (elemNullInd_) {
    char *resultElemIndPtr = resultElemPtr;

    // Set the indicator
    if (sourceAttr->getNullFlag() && sourceNullData == 0) {
      ExpTupleDesc::setNullValue(resultElemIndPtr, resultAttr->getNullBitIndex(), resultAttr->getTupleFormat());
      resultIsNull = TRUE;
    } else {
      ExpTupleDesc::clearNullValue(resultElemIndPtr, resultAttr->getNullBitIndex(), resultAttr->getTupleFormat());
    }
  } else if (sourceAttr->getNullFlag() && sourceNullData == 0) {
    // Source is null, but we do not have a way to express it
    ExRaiseSqlError(heap, diagsArea, EXE_MISSING_INDICATOR_VARIABLE);
    **diagsArea << DgSqlCode(-EXE_MISSING_INDICATOR_VARIABLE);
    return ex_expr::EXPR_ERROR;
  }

  // Copy the result if not null
  // For SQLVarChars, copy both val and len fields.
  if (resultIsNull == FALSE) {
    if (DFS2REC::isSQLVarChar(resultAttr->getDatatype())) {
      unsigned short VCLen = 0;
      str_cpy_all((char *)&VCLen, (char *)op_data[-ex_clause::MAX_OPERANDS + 1], resultAttr->getVCIndicatorLength());

      str_cpy_all(resultElemPtr + resultAttr->getNullIndicatorLength(), (char *)&VCLen,
                  resultAttr->getVCIndicatorLength());

      str_cpy_all(resultElemPtr + resultAttr->getNullIndicatorLength() + resultAttr->getVCIndicatorLength(), op_data[1],
                  VCLen);
    } else {
      str_cpy_all(resultElemPtr + resultAttr->getNullIndicatorLength(), op_data[1], resultAttr->getLength());
    }  // if isSQLVarChar
  }    // if resultIsNULL

  // Update the number of elements in the object associated with the array
  // and the array itself
  nrows++;
  str_cpy_all(op_data[0], (char *)&nrows, sizeof(int));

  return ex_expr::EXPR_OK;
}
ex_expr::exp_return_type ex_function_nullifzero::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgtOp = getOperand(0);
  char *tgt = op_data[0];
  char *tgtNull = op_data[-2 * MAX_OPERANDS];
  char *src = op_data[1];
  int srcLen = getOperand(1)->getLength();
  NABoolean resultIsNull = TRUE;
  for (int i = 0; i < srcLen; i++) {
    tgt[i] = src[i];
    if (src[i] != 0) {
      resultIsNull = FALSE;
    }
  }

  if (resultIsNull) {
    ExpTupleDesc::setNullValue(tgtNull, tgtOp->getNullBitIndex(), tgtOp->getTupleFormat());
  } else {
    ExpTupleDesc::clearNullValue(tgtNull, tgtOp->getNullBitIndex(), tgtOp->getTupleFormat());
  }

  return ex_expr::EXPR_OK;
}
//
// NVL(e1, e2) returns e2 if e1 is NULL otherwise e1. NVL(e1, e2) is
// equivalent to ANSI/ISO
//      COALESCE(e1, e2)
//        or,
//      CASE WHEN e1 IS NULL THEN e2 ELSE e1 END
// Both arguments can be nullable and actually null; they both can
// be constants as well.
// NVL() on CHAR type expressions is mapped to CASE. ISNULL(e1, e2) is
// mapped into NVL(e1, e2)
// Datatypes of e1 and e2 must be comparable/compatible.
//
ex_expr::exp_return_type ex_function_nvl::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // Common index into op_data[] to access Null Indicators
  int opNullIdx = -2 * MAX_OPERANDS;

  Attributes *tgtOp = getOperand(0);
  Attributes *arg1 = getOperand(1);
  Attributes *arg2 = getOperand(2);
  char *tgt = op_data[0];
  char *tgtNull = op_data[opNullIdx];
  char *src;
  UInt32 srcLen;
  NABoolean resultIsNull = TRUE;

  // As of today, NVL() on CHAR types becomes CASE. So make sure we are
  // not dealing with any CHAR types
  assert(!DFS2REC::isAnyCharacter(arg1->getDatatype()) && !DFS2REC::isAnyCharacter(arg2->getDatatype()));

  // Locate the operand that is not null: if both are null
  // resultIsNull will still be TRUE and we will just set the
  // NULL flag of the result. If any operand is NOT NULL we copy
  // that value into result and clear NULL flag of the result.

  if (!arg1->getNullFlag() || op_data[opNullIdx + 1]) {
    // First operand is either NOT NULLABLE or NON NULL Value.
    // This is the result.

    src = op_data[1];
    srcLen = arg1->getLength();
    resultIsNull = FALSE;
  } else {
    // Second operand could be the result, if it is not null.
    src = op_data[2];
    srcLen = arg2->getLength();

    // Second operand is either NOT NULLABLE or NON NULL Value.
    // This is the result.
    if (!arg2->getNullFlag() || op_data[opNullIdx + 2]) resultIsNull = FALSE;
  }

  if (resultIsNull) {
    // Result must be nullable
    assert(tgtOp->getNullFlag());
    ExpTupleDesc::setNullValue(tgtNull, tgtOp->getNullBitIndex(), tgtOp->getTupleFormat());
  } else {
    // clear nullflag of result if it is nullable
    if (tgtOp->getNullFlag()) ExpTupleDesc::clearNullValue(tgtNull, tgtOp->getNullBitIndex(), tgtOp->getTupleFormat());
  }

  // Copy src to result: this could be NULL
  assert((UInt32)(tgtOp->getLength()) >= srcLen);
  str_cpy_all(tgt, src, srcLen);

  return ex_expr::EXPR_OK;
}

// Get the first ngram string
#define TRIGRAM_LEN 3
ex_expr::exp_return_type ex_function_firstngram::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  // search for operand 1
#pragma nowarn(1506)  // warning elimination
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
#pragma warn(1506)  // warning elimination
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  int charLen = strlen(op_data[1]);
  TRGM *trg = generate_wildcard_trgm(op_data[1], charLen);
  trgm *trgData = GETARR(trg);
  int trgLen = ARRNELEM(trg);

  char tmp[TRIGRAM_LEN];
  if (trgLen > 0) {
    memcpy(tmp, (char *)trgData, TRIGRAM_LEN);
    tmp[TRIGRAM_LEN] = '\0';
    str_cpy_all(op_data[0], tmp, TRIGRAM_LEN);
    getOperand(0)->setVarLength(TRIGRAM_LEN, op_data[-MAX_OPERANDS]);
  } else {
    *(op_data[0]) = '\0';
    getOperand(0)->setVarLength(0, op_data[-MAX_OPERANDS]);
  }
  if (trg != NULL) free(trg);
  return ex_expr::EXPR_OK;
}

// compute the ngram count for a string
ex_expr::exp_return_type ex_function_ngramcount::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
  // search for operand 1
#pragma nowarn(1506)  // warning elimination
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
#pragma warn(1506)  // warning elimination
  if (cs == CharInfo::UTF8) {
    int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
    len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
  }

  int charLen = strlen(op_data[1]);
  TRGM *trg = generate_wildcard_trgm(op_data[1], charLen);
  // trgm *trgData = GETARR(trg);
  int trgLen = ARRNELEM(trg);

  *(int *)op_data[0] = trgLen;
  return ex_expr::EXPR_OK;
}

//
// Clause used to clear header bytes for both disk formats
// SQLMX_FORMAT and SQLMX_ALIGNED_FORMAT.  The number of bytes to clear
// is different for both formats.
// This clause is only generated for insert expressions and update expressions
// (updates that are non-optimized since olt optimized updates do a strcpy
// of the old image and then update the specific columns).
ex_expr::exp_return_type ExHeaderClause::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *tgtData = op_data[0];
  Attributes *tgtOp = getOperand(0);

  // Clear the entire header (not the VOA area)
  str_pad(tgtData, (int)adminSz_, '\0');

  if (bitmapOffset_ > 0) ((ExpAlignedFormat *)tgtData)->setBitmapOffset(bitmapOffset_);

  // Can not use the tgt attributes offset value here since for the aligned
  // format this may not be the first fixed field since the fixed fields
  // are re-ordered.
  if (isSQLMXAlignedFormat())
    ((ExpAlignedFormat *)tgtData)->setFirstFixedOffset(firstFixedOffset_);
  else
    ExpTupleDesc::setFirstFixedOffset(tgtData, firstFixedOffset_, tgtOp->getTupleFormat());

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ex_function_queryid_extract::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int retcode = 0;

  char *qidStr = op_data[1];
  char *attrStr = op_data[2];
  int qidLen = getOperand(1)->getLength();
  int attrLen = getOperand(2)->getLength();

  int attr = -999;
  NABoolean isNumeric = FALSE;

  // remove trailing blanks from attrStr
  while (attrLen && attrStr[attrLen - 1] == ' ') attrLen--;

  if (strncmp(attrStr, "SEGMENTNUM", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_SEGMENTNUM;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "CPU", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_CPUNUM;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "CPUNUM", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_CPUNUM;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "PIN", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_PIN;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "EXESTARTTIME", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_EXESTARTTIME;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "SESSIONID", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_SESSIONID;
  } else if (strncmp(attrStr, "SESSIONNUM", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_SESSIONNUM;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "USERNAME", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_USERNAME;
  } else if (strncmp(attrStr, "TENANTID", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_TENANTID;
  } else if (strncmp(attrStr, "SESSIONNAME", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_SESSIONNAME;
  } else if (strncmp(attrStr, "QUERYNUM", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_QUERYNUM;
    isNumeric = TRUE;
  } else if (strncmp(attrStr, "STMTNAME", attrLen) == 0) {
    attr = ComSqlId::SQLQUERYID_STMTNAME;
  }

  long value;
  if (!isNumeric) value = 99;  // set max valueStr length
  char valueStr[100];
  retcode = ComSqlId::getSqlQueryIdAttr(attr, qidStr, qidLen, value, valueStr);
  if (retcode < 0) {
    ExRaiseFunctionSqlError(heap, diagsArea, (ExeErrorCode)(-retcode), derivedFunction(), origFunctionOperType());
    return ex_expr::EXPR_ERROR;
  }

  char *valPtr;
  short datatype;
  int length;
  if (isNumeric) {
    valPtr = (char *)&value;
    datatype = REC_BIN64_SIGNED;
    length = 8;
  } else {
    valPtr = valueStr;
    datatype = REC_BYTE_V_ANSI;
    length = (int)value + 1;  // include null terminator
  }

  if (convDoIt(valPtr, length, datatype, 0, 0, op_data[0], getOperand(0)->getLength(), getOperand(0)->getDatatype(),
               getOperand(0)->getPrecision(), getOperand(0)->getScale(), op_data[-MAX_OPERANDS],
               getOperand(0)->getVCIndicatorLength(), heap, diagsArea))
    return ex_expr::EXPR_ERROR;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionUniqueId::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int retcode = 0;

  char *result = op_data[0];
  if (getOperType() == ITM_UNIQUE_ID) {
    // it is hard to find a common header file for these length
    // so hardcode 36 here
    // if change, please check the SynthType.cpp for ITM_UNIQUE_ID part as well
    // libuuid is global unique, even across computer node
    // NOTE: libuuid is avialble on normal CentOS, other system like Ubuntu may need to check
    // Trafodion only support RHEL and CentOS as for now
    char str[36 + 1];
    uuid_t uu;
    uuid_generate(uu);
    uuid_unparse(uu, str);
    str_cpy_all(result, str, 36);
  } else if (getOperType() == ITM_UNIQUE_ID_SYS_GUID) {
    uuid_t uu;
    uuid_generate(uu);
    str_cpy_all(result, (char *)&uu, sizeof(uu));
  } else  // at present , it must be ITM_UUID_SHORT_ID
  {
    long uniqueUID;

    ComUID comUID;
    comUID.make_UID();

#if defined(NA_LITTLE_ENDIAN)
    uniqueUID = reversebytes(comUID.get_value());
#else
    uniqueUID = comUID.get_value();
#endif

    // it is safe, since the result is allocated 21 bytes in this case from synthtype,
    // max in64 is 19 digits and one for sign, 21 is enough
    sprintf(result, "%lu", uniqueUID);
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionRowNum::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *result = op_data[0];

  long rowNum = getExeGlobals()->rowNum();

  str_cpy_all(result, (char *)&rowNum, sizeof(long));
  str_pad(&result[sizeof(long)], sizeof(long), '\0');

  return ex_expr::EXPR_OK;
}

short ExFunctionHbaseColumnLookup::extractColFamilyAndName(const char *input, short len, NABoolean isVarchar,
                                                           std::string &colFam, std::string &colName) {
  if (!input) return -1;

  int i = 0;
  int startPos = 0;
  if (isVarchar) {
    len = *(short *)input;
    startPos = sizeof(len);
  } else if (len == -1) {
    len = strlen(input);
    startPos = 0;
  } else {
    startPos = 0;
  }

  int j = 0;
  i = startPos;
  NABoolean colonFound = FALSE;
  while ((j < len) && (not colonFound)) {
    if (input[i] != ':') {
      i++;
    } else {
      colonFound = TRUE;
    }

    j++;
  }

  if (colonFound)  // ":" found
  {
    colFam.assign(&input[startPos], i - startPos);

    i++;
    if (i < (startPos + len)) {
      colName.assign(&input[i], (startPos + len) - i);
    }
  } else {
    colName.assign(&input[startPos], i - startPos);
  }

  return 0;
}

ex_expr::exp_return_type ExFunctionHbaseColumnLookup::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // op_data[0] points to result. The result is a varchar.
  Attributes *resultAttr = getOperand(0);
  Attributes *colDetailAttr = getOperand(1);

  char *resultStart = op_data[0];
  char *resultNull = op_data[-2 * MAX_OPERANDS];

  char *result = resultStart;
  char *colDetail = op_data[1];

  int sourceLen = 0;
  if (colDetailAttr->getVCIndicatorLength() == sizeof(int))
    str_cpy_all((char *)&sourceLen, op_data[-MAX_OPERANDS + 1], sizeof(int));
  else {
    short tempLen = 0;
    str_cpy_all((char *)&tempLen, op_data[-MAX_OPERANDS + 1], sizeof(short));
    sourceLen = tempLen;
  }

  char *pos = colDetail;
  NABoolean done = FALSE;
  NABoolean colFound = FALSE;
  while (NOT done) {
    short colNameLen = 0;
    int colValueLen = 0;

    memcpy((char *)&colNameLen, pos, sizeof(short));
    pos += sizeof(short);

    if ((colNameLen == strlen(colName_)) && (str_cmp(colName_, pos, colNameLen) == 0)) {
      pos += colNameLen;

      memcpy((char *)&colValueLen, pos, sizeof(int));
      pos += sizeof(int);

      NABoolean charType = DFS2REC::isAnyCharacter(resultAttr->getDatatype());
      if (!charType) {
        // lengths must match for non-char types
        if (colValueLen != resultAttr->getLength()) continue;
      }

      UInt32 flags = 0;

      ex_expr::exp_return_type rc =
          convDoIt(pos, colValueLen, (charType ? REC_BYTE_F_ASCII : resultAttr->getDatatype()),
                   (charType ? 0 : resultAttr->getPrecision()), (charType ? 0 : resultAttr->getScale()), result,
                   resultAttr->getLength(), resultAttr->getDatatype(), resultAttr->getPrecision(),
                   resultAttr->getScale(), NULL, 0, heap, diagsArea);
      if ((rc != ex_expr::EXPR_OK) ||
          ((diagsArea) && (*diagsArea) && ((*diagsArea)->getNumber(DgSqlCode::WARNING_)) > 0)) {
        if (rc == ex_expr::EXPR_OK) {
          (*diagsArea)->negateAllWarnings();
        }

        return ex_expr::EXPR_ERROR;
      }

      getOperand(0)->setVarLength(colValueLen, op_data[-MAX_OPERANDS]);

      colFound = TRUE;

      done = TRUE;
    } else {
      pos += colNameLen;

      memcpy((char *)&colValueLen, pos, sizeof(int));
      pos += sizeof(int);

      pos += colValueLen;

      if (pos >= (colDetail + sourceLen)) {
        done = TRUE;
      }
    }
  }  // while

  if (NOT colFound) {
    // move null value to result
    ExpTupleDesc::setNullValue(resultNull, resultAttr->getNullBitIndex(), resultAttr->getTupleFormat());
  } else {
    ExpTupleDesc::clearNullValue(resultNull, resultAttr->getNullBitIndex(), resultAttr->getTupleFormat());
  }

  return ex_expr::EXPR_OK;
}

NABoolean ExFunctionHbaseColumnsDisplay::toBeDisplayed(char *colName, int colNameLen) {
  if ((!colNames()) || (numCols_ == 0)) return TRUE;

  char *currColName = colNames();
  for (int i = 0; i < numCols_; i++) {
    short currColNameLen = *(short *)currColName;
    currColName += sizeof(short);
    if ((colNameLen == currColNameLen) && (memcmp(colName, currColName, colNameLen) == 0)) return TRUE;

    currColName += currColNameLen;
  }

  return FALSE;
}

ex_expr::exp_return_type ExFunctionHbaseColumnsDisplay::eval(char *op_data[], CollHeap *heap,
                                                             ComDiagsArea **diagsArea) {
  // op_data[0] points to result. The result is a varchar.
  Attributes *resultAttr = getOperand(0);
  Attributes *colDetailAttr = getOperand(1);

  char *resultStart = op_data[0];
  char *result = resultStart;
  char *colDetail = op_data[1];

  int sourceLen = 0;
  if (colDetailAttr->getVCIndicatorLength() == sizeof(int))
    str_cpy_all((char *)&sourceLen, op_data[-MAX_OPERANDS + 1], sizeof(int));
  else {
    short tempLen = 0;
    str_cpy_all((char *)&tempLen, op_data[-MAX_OPERANDS + 1], sizeof(short));
    sourceLen = tempLen;
  }

  char *pos = colDetail;
  NABoolean done = FALSE;

  while (NOT done) {
    short colNameLen = 0;
    int colValueLen = 0;

    memcpy((char *)&colNameLen, pos, sizeof(short));
    pos += sizeof(short);
    memcpy(result, pos, colNameLen);
    pos += colNameLen;

    // if this col name need to be returned, then return it.
    if (NOT toBeDisplayed(result, colNameLen)) {
      goto label_continue;
    }

    result += colNameLen;

    memcpy(result, " => ", strlen(" => "));
    result += strlen(" => ");

    memcpy((char *)&colValueLen, pos, sizeof(int));
    pos += sizeof(int);
    memcpy(result, pos, colValueLen);
    result += colValueLen;
    pos += colValueLen;

    if (pos < (colDetail + sourceLen)) {
      memcpy(result, ", ", strlen(", "));
      result += strlen(", ");
    }

  label_continue:
    if (pos >= (colDetail + sourceLen)) {
      done = TRUE;
    }
  }

  // store the row length in the varlen indicator.
  getOperand(0)->setVarLength((result - resultStart), op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionHbaseColumnCreate::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // op_data[0] points to result. The result is a varchar.
  // Values in result have already been populated by clauses evaluated
  // before this clause is reached.
  Attributes *resultAttr = getOperand(0);
  char *resultStart = op_data[0];
  char *result = resultStart;

  str_cpy_all(result, (char *)&numEntries_, sizeof(numEntries_));
  result += sizeof(short);

  str_cpy_all(result, (char *)&colNameMaxLen_, sizeof(colNameMaxLen_));
  result += sizeof(short);

  str_cpy_all(result, (char *)&colValVCIndLen_, sizeof(colValVCIndLen_));
  result += sizeof(short);

  str_cpy_all(result, (char *)&colValMaxLen_, sizeof(colValMaxLen_));
  result += sizeof(int);

  for (int i = 0; i < numEntries_; i++) {
    // validate that column name is of right format:   colfam:colname
    std::string colFam;
    std::string colNam;
    ExFunctionHbaseColumnLookup::extractColFamilyAndName(result, -1, TRUE /*isVarchar*/, colFam, colNam);
    if (colFam.empty()) {
      short colNameLen;
      str_cpy_all((char *)&colNameLen, result, sizeof(short));
      result += sizeof(short);
      std::string colNamData(result, colNameLen);
      ExRaiseSqlError(heap, diagsArea, (ExeErrorCode)1426, NULL, NULL, NULL, NULL, colNamData.data());
      return ex_expr::EXPR_ERROR;
    }

    result += sizeof(short);
    result += ROUND2(colNameMaxLen_);

    // skip the nullable bytes
    result += sizeof(short);

    if (colValVCIndLen_ == sizeof(short))
      result += sizeof(short);
    else {
      result = (char *)ROUND4((long)result);
      result += sizeof(int);
    }
    result += ROUND2(colValMaxLen_);
  }

  resultAttr->setVarLength(result - resultStart, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionCastType::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  // op_data[0] points to result.
  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  char *resultData = op_data[0];
  char *srcData = op_data[1];

  int sourceLen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);
  int resultLen = resultAttr->getLength();

  if (sourceLen < resultLen) {
    ExRaiseFunctionSqlError(heap, diagsArea, EXE_STRING_OVERFLOW, derivedFunction(), origFunctionOperType());
    return ex_expr::EXPR_ERROR;
  }

  NABoolean wasSwapped = FALSE;

#if defined(NA_LITTLE_ENDIAN)
  if (DFS2REC::isAnyCharacter(srcAttr->getDatatype())) {
    wasSwapped = swapBytes(resultAttr->getDatatype(), srcData, resultData);
  }
#endif

  if (NOT wasSwapped) str_cpy_all(resultData, srcData, resultLen);

  //  str_cpy_all(resultData, srcData, resultLen);
  getOperand(0)->setVarLength(resultLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionSequenceValue::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  short rc = 0;

  // op_data[0] points to result. The result is a varchar.
  Attributes *resultAttr = getOperand(0);
  char *result = op_data[0];

  SequenceValueGenerator *seqValGen = getExeGlobals()->seqGen();
  seqValGen->setRetryNum(getRetryNum());
  seqValGen->setUseDlockImpl(useDlockImpl());
  seqValGen->setSkipWalForIncrColVal(isSkipWalForIncrColVal());
  seqValGen->setUseDtmImpl(useDtmImpl());
  long seqVal = 0;
  if ((isCurr()) && (sga_.isSystemSG())) {
    ExRaiseSqlError(heap, diagsArea, (ExeErrorCode)ABS(-1592));
    *(*diagsArea) << DgString0("CURRENT") << DgString1("SYSTEM SEQUENCE ");
    return ex_expr::EXPR_ERROR;
  }
  if (sga_.isSystemSG()) {
    rc = seqValGen->getIdtmSeqVal(sga_, seqVal, diagsArea);
    if (rc)
      return ex_expr::EXPR_ERROR;  // diags is already populated
    else
      *(long *)result = seqVal;
    return ex_expr::EXPR_OK;
  }

  if (isCurr())
    rc = seqValGen->getCurrSeqVal(sga_, seqVal);
  else
    rc = seqValGen->getNextSeqVal(sga_, seqVal);

  if (rc) {
    QRLogger::log(CAT_SQL_EXE, LL_ERROR, "Sequence %s returned error %d: seqUid = %ld",
                  isCurr() ? "currval" : "nextval", rc, sga_.getSGObjectUID());
    ExRaiseSqlError(heap, diagsArea, (ExeErrorCode)ABS(rc));
    return ex_expr::EXPR_ERROR;
  }

  *(long *)result = seqVal;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionHbaseVisibility::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  short rc = 0;

  // op_data[0] points to result.
  Attributes *resultAttr = getOperand(0);
  char *result = op_data[0];

  long *hbaseTS = (long *)op_data[1];

  *(long *)result = hbaseTS[colIndex_];

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionHbaseVisibilitySet::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  short rc = 0;

  // op_data[0] points to result.
  Attributes *resultAttr = getOperand(0);
  char *result = op_data[0];

  char *currPos = result;

  *(short *)currPos = colIDlen_;
  currPos += sizeof(short);

  memcpy(currPos, colID_, colIDlen_);
  currPos += ROUND2(colIDlen_);

  *(int *)currPos = visExprLen_;
  currPos += sizeof(int);

  memcpy(currPos, visExpr(), visExprLen_);
  currPos += visExprLen_;

  //  resultAttr->setVarLength((currPos - result), op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

// Do a converttohex() for ROWID, so we don't need to add a explicit
// converttohex() when we use ROWID in select list or where clause.
ex_expr::exp_return_type ExFunctionHbaseRowid::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  static const char HexArray[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

  int len0 = getOperand(0)->getLength(op_data[-MAX_OPERANDS]);
  int len1 = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  assert(len0 >= len1);

  int i;
  if (DFS2REC::isDoubleCharacter(getOperand(1)->getDatatype())) {
    NAWchar *w_p = (NAWchar *)op_data[1];
    int w_len = len1 / sizeof(NAWchar);
    for (i = 0; i < w_len; i++) {
      op_data[0][4 * i] = HexArray[0x0F & w_p[i] >> 12];
      op_data[0][4 * i + 1] = HexArray[0x0F & w_p[i] >> 8];
      op_data[0][4 * i + 2] = HexArray[0x0F & w_p[i] >> 4];
      op_data[0][4 * i + 3] = HexArray[0x0F & w_p[i]];
    }
  } else {
    CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();
    if (cs == CharInfo::UTF8) {
      int prec1 = ((SimpleType *)getOperand(1))->getPrecision();
      len1 = Attributes::trimFillerSpaces(op_data[1], prec1, len1, cs);
    }

    char *srcData = op_data[1];
#if defined(NA_LITTLE_ENDIAN)
    long temp;
    if (swapBytes(getOperand(1)->getDatatype(), srcData, (char *)&temp)) srcData = (char *)&temp;
#endif

    for (i = 0; i < len1; i++) {
      op_data[0][2 * i] = HexArray[0x0F & srcData[i] >> 4];
      op_data[0][2 * i + 1] = HexArray[0x0F & srcData[i]];
    }
  }

  getOperand(0)->setVarLength(2 * len1, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionHbaseTimestamp::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  short rc = 0;

  // op_data[0] points to result.
  Attributes *resultAttr = getOperand(0);
  char *result = op_data[0];

  long *hbaseTS = (long *)op_data[1];

  *(long *)result = hbaseTS[colIndex_];

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionHbaseVersion::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  short rc = 0;

  // op_data[0] points to result.
  Attributes *resultAttr = getOperand(0);
  char *result = op_data[0];

  long *hbaseVersion = (long *)op_data[1];

  *(long *)result = hbaseVersion[colIndex_];

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////////////////
//
// decodeKeyValue
//
// This routine decodes an encoded key value.
//
// Note: The target MAY point to the source to change the original
//       value.
//
////////////////////////////////////////////////////////////////////
short ex_function_encode::decodeKeyValue(Attributes *attr, NABoolean isDesc, char *inSource, char *varlen_ptr,
                                         char *target, char *target_varlen_ptr, NABoolean handleNullability) {
  int fsDatatype = attr->getDatatype();
  int length = attr->getLength();
  int precision = attr->getPrecision();

  int encodedKeyLen = length;
  if ((handleNullability) && (attr->getNullFlag())) encodedKeyLen += attr->getNullIndicatorLength();

  char *source = inSource;
  if (isDesc) {
    // compliment all bytes
    for (int k = 0; k < encodedKeyLen; k++) target[k] = ~(source[k]);

    source = target;
  }

  if ((handleNullability) && (attr->getNullFlag())) {
    if (target != source) str_cpy_all(target, source, attr->getNullIndicatorLength());

    source += attr->getNullIndicatorLength();
    target += attr->getNullIndicatorLength();
  }

  switch (fsDatatype) {
#if defined(NA_LITTLE_ENDIAN)
    case REC_BIN8_SIGNED:
      //
      // Flip the sign bit.
      //
      *(UInt8 *)target = *(UInt8 *)source;
      target[0] ^= 0200;
      break;

    case REC_BIN8_UNSIGNED:
    case REC_BOOLEAN:
      *(UInt8 *)target = *(UInt8 *)source;
      break;

    case REC_BIN16_SIGNED:
      //
      // Flip the sign bit.
      //
      *((unsigned short *)target) = reversebytes(*((unsigned short *)source));
      target[sizeof(short) - 1] ^= 0200;
      break;

    case REC_BPINT_UNSIGNED:
    case REC_BIN16_UNSIGNED:
      *((unsigned short *)target) = reversebytes(*((unsigned short *)source));
      break;

    case REC_BIN32_SIGNED:
      //
      // Flip the sign bit.
      //
      *((int *)target) = reversebytes(*((int *)source));
      target[sizeof(int) - 1] ^= 0200;
      break;

    case REC_BIN32_UNSIGNED:
      *((int *)target) = reversebytes(*((int *)source));
      break;

    case REC_BIN64_SIGNED:
      //
      // Flip the sign bit.
      //
      *((long long *)target) = reversebytes(*((long long *)source));
      target[sizeof(long long) - 1] ^= 0200;
      break;

    case REC_BIN64_UNSIGNED:
      *((UInt64 *)target) = reversebytes(*((UInt64 *)source));
      break;

    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND:
      switch (length) {
        case 2:  // Signed 16 bit
          *((unsigned short *)target) = reversebytes(*((unsigned short *)source));
          target[SQL_SMALL_SIZE - 1] ^= 0200;
          break;
        case 4:  // Signed 32 bit
          *((int *)target) = reversebytes(*((int *)source));
          target[SQL_INT_SIZE - 1] ^= 0200;
          break;
        case 8:  // Signed 64 bit
          *((long long *)target) = reversebytes(*((long long *)source));
          target[SQL_LARGE_SIZE - 1] ^= 0200;
          break;
        default:
          assert(FALSE);
          break;
      };  // switch(length)
      break;
    case REC_DATETIME: {
      // This method has been modified as part of the MP Datetime
      // Compatibility project.  It has been made more generic so that
      // it depends only on the start and end fields of the datetime type.
      //
      rec_datetime_field startField;
      rec_datetime_field endField;

      ExpDatetime *dtAttr = (ExpDatetime *)attr;

      // Get the start and end fields for this Datetime type.
      //
      dtAttr->getDatetimeFields(dtAttr->getPrecision(), startField, endField);

      // Copy all of the source to the destination, then reverse only
      // those fields of the target that are longer than 1 byte
      //
      if (target != source) str_cpy_all(target, source, length);

      // Reverse the YEAR and Fractional precision fields if present.
      //
      char *ptr = target;
      for (int field = startField; field <= endField; field++) {
        switch (field) {
          case REC_DATE_YEAR:
            // convert YYYY from little endian to big endian
            //
            *((unsigned short *)ptr) = reversebytes(*((unsigned short *)ptr));
            ptr += sizeof(short);
            break;
          case REC_DATE_MONTH:
          case REC_DATE_DAY:
          case REC_DATE_HOUR:
          case REC_DATE_MINUTE:
            // One byte fields are copied as is...
            ptr++;
            break;
          case REC_DATE_SECOND:
            ptr++;

            // if there is a fraction, make it big endian
            // (it is an unsigned long, beginning after the SECOND field)
            //
            if (dtAttr->getScale() > 0) *((int *)ptr) = reversebytes(*((int *)ptr));
            break;
        }
      }
      break;
    }
#else
    case REC_BIN8_SIGNED:
    case REC_BIN16_SIGNED:
    case REC_BIN32_SIGNED:
    case REC_BIN64_SIGNED:
    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND:
      //
      // Flip the sign bit.
      //
      if (target != source) str_cpy_all(target, source, length);
      target[0] ^= 0200;
      break;
#endif
    case REC_DECIMAL_LSE:
      //
      // If the number was negative, complement all the bytes.  Otherwise, set
      // the sign bit.
      //
      if (NOT(source[0] & 0200)) {
        for (int i = 0; i < length; i++) target[i] = ~source[i];
      } else {
        if (target != source) str_cpy_all(target, source, length);
        target[0] &= ~0200;
      }
      break;
    case REC_NUM_BIG_SIGNED:
    case REC_NUM_BIG_UNSIGNED: {
      BigNum type(length, precision, 0, 0);
      type.decode(source, target);
      break;
    }
    case REC_IEEE_FLOAT32: {
      //
      // Encoded float (IEEE 754 - 1985 standard):
      //
      // +-+--------+-----------------------+
      // | |Exponent| Mantissa              |
      // | |(8 bits)| (23 bits)             |
      // +-+--------+-----------------------+
      //  ||                                |
      //  |+- Complemented if sign was neg.-+
      //  |
      //  +- Sign bit complement
      //
      // unencoded float (IEEE 754 - 1985 standard):
      //
      // +-+----------+---------------------+
      // | | exponent |  mantissa           |
      // | | (8 bits) |  (23 bits)          |
      // +-+----------+---------------------+
      //  |
      //  +- Sign bit
      //

      // the following code is independent of the "endianess" of the
      // archtiecture. Instead, it assumes IEEE 754 - 1985 standard
      // for representation of floats
      if (source[0] & 0200) {
        // sign bit is on. Indicates this was a positive number.
        // Copy to target and clear the sign bit.
        if (target != source) str_cpy_all(target, source, length);
        target[0] &= 0177;
      } else {
        // this was a negative number.
        // flip all bits.
        for (int i = 0; i < length; i++) target[i] = ~source[i];
      }

      // here comes the dependent part
#ifdef NA_LITTLE_ENDIAN
      *(int *)target = reversebytes(*(int *)target);
#endif
      break;
    }
    case REC_IEEE_FLOAT64: {
      //
      // Encoded double (IEEE 754 - 1985 standard):
      //
      // +-+-----------+--------------------+
      // | | Exponent  | Mantissa           |
      // | | (11 bits) | (52 bits)          |
      // +-+-----------+--------------------+
      //  ||                                |
      //  |+- Complemented if sign was neg.-+
      //  |
      //  +- Sign bit complement
      //
      // unencoded double (IEEE 754 - 1985 standard):
      //
      // +-+--------- -+--------------------+
      // | | exponent  |  mantissa          |
      // | | (11 bits) |  (52 bits)         |
      // +-+--------- -+--------------------+
      //  |
      //  +- Sign bit
      //

      // the following code is independent of the "endianess" of the
      // archtiecture. Instead, it assumes IEEE 754 - 1985 standard
      // for representation of floats
      if (source[0] & 0200) {
        // sign bit is on. Indicates this was a positive number.
        // Copy to target and clear the sign bit.
        if (target != source) str_cpy_all(target, source, length);
        target[0] &= 0177;
      } else {
        // this was a negative number.
        // flip all bits.
        for (int i = 0; i < length; i++) target[i] = ~source[i];
      }

      // here comes the dependent part
#ifdef NA_LITTLE_ENDIAN
      *(long *)target = reversebytes(*(long *)target);
#endif

      break;
    }
    case REC_BYTE_V_ASCII:
    case REC_BYTE_V_ASCII_LONG: {
      //
      // Copy the source to the target.
      //
      short vc_len;
      // See bug LP 1444134, make this compatible with encoding for
      // varchars and remove the VC indicator
      assert(attr->getVCIndicatorLength() == sizeof(vc_len));
      str_cpy_all((char *)&vc_len, varlen_ptr, attr->getVCIndicatorLength());

      if (target != source) str_cpy_all(target, source, vc_len);
      //
      // Blankpad the target (if needed).
      //
      if (vc_len < length) str_pad(&target[vc_len], (int)(length - vc_len), ' ');
      //
      // Make the length bytes to be the maximum length for this field.  This
      // will make all encoded varchar keys to have the same length and so the
      // comparison will depend on the fixed part of the varchar buffer.
      //
      vc_len = (short)length;
      if (target_varlen_ptr) str_cpy_all(target_varlen_ptr, (char *)&vc_len, attr->getVCIndicatorLength());
      break;
    }

    case REC_NCHAR_V_UNICODE: {
      //
      // Copy the source to the target.
      //
      // See bug LP 1444134, make this compatible with encoding for
      // varchars and remove the VC indicator
      short vc_len;
      assert(attr->getVCIndicatorLength() == sizeof(vc_len));
      str_cpy_all((char *)&vc_len, varlen_ptr, attr->getVCIndicatorLength());

      if (target != source) str_cpy_all(target, source, vc_len);
      //
      // Blankpad the target (if needed).
      //
      if (vc_len < length)
        wc_str_pad((NAWchar *)&target[attr->getVCIndicatorLength() + vc_len], (int)(length - vc_len) / sizeof(NAWchar),
                   unicode_char_set::space_char());

#if defined(NA_LITTLE_ENDIAN)
      wc_swap_bytes((NAWchar *)&target[attr->getVCIndicatorLength()], length / sizeof(NAWchar));
#endif
      //
      // Make the length bytes to be the maximum length for this field.  This
      // will make all encoded varchar keys to have the same length and so the
      // comparison will depend on the fixed part of the varchar buffer.
      //
      vc_len = (short)length;
      if (target_varlen_ptr) str_cpy_all(target_varlen_ptr, (char *)&vc_len, attr->getVCIndicatorLength());
      break;
    }

    case REC_NCHAR_F_UNICODE: {
      if (target != source) str_cpy_all(target, source, length);

#if defined(NA_LITTLE_ENDIAN)
      wc_swap_bytes((NAWchar *)target, length / sizeof(NAWchar));
#endif

      break;
    }
    default:
      //
      // Decoding is not needed.  Just copy the source to the target.
      //
      if (target != source) str_cpy_all(target, source, length);
      break;
  }

  return 0;
}

static int convAsciiLength(Attributes *attr) {
  int d_len = 0;
  int scale_len = 0;

  int datatype = attr->getDatatype();
  int length = attr->getLength();
  int precision = attr->getPrecision();
  int scale = attr->getScale();

  if (scale > 0) scale_len = 1;

  switch (datatype) {
    case REC_BPINT_UNSIGNED:
      // Can set the display size based on precision. For now treat it as
      // unsigned smallint
      d_len = SQL_USMALL_DISPLAY_SIZE;
      break;

    case REC_BIN16_SIGNED:
      d_len = SQL_SMALL_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN16_UNSIGNED:
      d_len = SQL_USMALL_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN32_SIGNED:
      d_len = SQL_INT_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN32_UNSIGNED:
      d_len = SQL_UINT_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN64_SIGNED:
      d_len = SQL_LARGE_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN64_UNSIGNED:
      d_len = SQL_ULARGE_DISPLAY_SIZE + scale_len;
      break;

    case REC_NUM_BIG_UNSIGNED:
    case REC_NUM_BIG_SIGNED:
      d_len = precision + 1 + scale_len;  // Precision + sign + decimal point
      break;

    case REC_BYTE_F_ASCII:
      d_len = length;
      break;

    case REC_NCHAR_F_UNICODE:
    case REC_NCHAR_V_UNICODE:
    case REC_BYTE_V_ASCII:
    case REC_BYTE_V_ASCII_LONG:
      d_len = length;
      break;

    case REC_DECIMAL_UNSIGNED:
      d_len = length + scale_len;
      break;

    case REC_DECIMAL_LSE:
      d_len = length + 1 + scale_len;
      break;

    case REC_FLOAT32:
      d_len = SQL_REAL_DISPLAY_SIZE;
      break;

    case REC_FLOAT64:
      d_len = SQL_DOUBLE_PRECISION_DISPLAY_SIZE;
      break;

    case REC_DATETIME:
      switch (precision) {
          // add different literals for sqldtcode_date...etc. These literals
          // are from sqlcli.h and cannot be included here in this file.
        case 1 /*SQLDTCODE_DATE*/: {
          d_len = DATE_DISPLAY_SIZE;
        } break;
        case 2 /*SQLDTCODE_TIME*/: {
          d_len = TIME_DISPLAY_SIZE + (scale > 0 ? (1 + scale) : 0);
        } break;
        case 3 /*SQLDTCODE_TIMESTAMP*/: {
          d_len = TIMESTAMP_DISPLAY_SIZE + (scale > 0 ? (1 + scale) : 0);
        } break;
        default:
          d_len = length;
          break;
      }
      break;

    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND: {
      rec_datetime_field startField;
      rec_datetime_field endField;
      ExpInterval::getIntervalStartField(datatype, startField);
      ExpInterval::getIntervalEndField(datatype, endField);

      // this code is copied from IntervalType::getStringSize in
      // w:/common/IntervalType.cpp
      d_len = 1 + 1 + precision + 3 /*IntervalFieldStringSize*/ * (endField - startField);
      if (scale) d_len += scale + 1;  // 1 for "."
    } break;

    default:
      d_len = length;
      break;
  }

  return d_len;
}

// helper function, convert a string into IPV4 , if valid, it can support leading and padding space
static int string2ipv4(char *srcData, int slen, unsigned int *inet_addr) {
  Int16 i = 0, j = 0, p = 0, leadingspace = 0;
  char buf[16];
  Int16 dot = 0;

  if (slen < MIN_IPV4_STRING_LEN) return 0;

  unsigned char *ipv4_bytes = (unsigned char *)inet_addr;

  if (srcData[0] == ' ') {
    char *next = srcData;
    while (*next == ' ') {
      leadingspace++;
      next++;
    }
  }

  for (i = leadingspace, j = 0; i < slen; i++) {
    if (srcData[i] == '.') {
      buf[j] = 0;
      p = str_atoi(buf, j);
      if (p < 0 || p > 255 || j == 0) {
        return 0;
      } else {
        if (ipv4_bytes) ipv4_bytes[dot] = (unsigned char)p;
      }
      j = 0;
      dot++;
      if (dot > 3) return 0;
    } else if (srcData[i] == ' ') {
      break;  // space is terminator
    } else {
      if (isdigit(srcData[i]) == 0) {
        return 0;
      } else
        buf[j] = srcData[i];
      j++;
    }
  }
  Int16 stoppos = i;

  // the last part
  buf[j] = 0;  // null terminator

  for (i = 0; i < j; i++)  // check for invalid character
  {
    if (isdigit(buf[i]) == 0) {
      return 0;
    }
  }
  p = str_atoi(buf, j);
  if (p < 0 || p > 255 || j == 0)  // check for invalid number
  {
    return 0;
  } else {
    if (ipv4_bytes) ipv4_bytes[dot] = (unsigned char)p;
  }

  // if terminated by space
  if (stoppos < slen - 1) {
    for (j = stoppos; j < slen; j++) {
      if (srcData[j] != ' ') return 0;
    }
  }

  if (dot != 3)
    return 0;
  else
    return 1;
}

ex_expr::exp_return_type ExFunctionInetAton::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  char *srcData = op_data[1];
  char *resultData = op_data[0];

  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  int slen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);
  int rlen = resultAttr->getLength();

  unsigned int addr;
  int ret = string2ipv4(srcData, slen, &addr);
  if (ret) {
    *(unsigned int *)op_data[0] = addr;
    return ex_expr::EXPR_OK;
  } else {
    ExRaiseSqlError(heap, diags, EXE_INVALID_CHARACTER);
    *(*diags) << DgString0("IP format") << DgString1("INET_ATON FUNCTION");
    return ex_expr::EXPR_ERROR;
  }
}

ex_expr::exp_return_type ExFunctionInetNtoa::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  char buf[16];  // big enough
  unsigned long addr = *(unsigned long *)op_data[1];
  char *resultData = op_data[0];
  Attributes *resultAttr = getOperand(0);
  const unsigned char *ipv4_bytes = (const unsigned char *)&addr;

  if (addr > 4294967295) {
    ExRaiseSqlError(heap, diags, EXE_BAD_ARG_TO_MATH_FUNC);
    *(*diags) << DgString0("INET_NTOA");
    return ex_expr::EXPR_ERROR;
  }

  str_sprintf(buf, "%d.%d.%d.%d", ipv4_bytes[0], ipv4_bytes[1], ipv4_bytes[2], ipv4_bytes[3]);
  int slen = str_len(buf);
  str_cpy_all(resultData, buf, slen);
  getOperand(0)->setVarLength(slen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionCrc32::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  int slen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);
  int rlen = resultAttr->getLength();

  *(int *)op_data[0] = 0;
  int crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef *)op_data[1], slen);
  *(int *)op_data[0] = crc;
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionSha2::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  unsigned char sha[SHA512_DIGEST_LENGTH + 1] = {0};

  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  int slen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);

  // the length of result
  int rlen = SHA512_DIGEST_LENGTH;

  switch (mode) {
    case 0:
    case 256:
      SHA256_CTX sha_ctx_256;
      if (!SHA256_Init(&sha_ctx_256)) goto sha2_error;
      if (!SHA256_Update(&sha_ctx_256, op_data[1], slen)) goto sha2_error;
      if (!SHA256_Final((unsigned char *)sha, &sha_ctx_256)) goto sha2_error;

      rlen = SHA256_DIGEST_LENGTH;
      break;

    case 224:
      SHA256_CTX sha_ctx_224;

      if (!SHA224_Init(&sha_ctx_224)) goto sha2_error;
      if (!SHA224_Update(&sha_ctx_224, op_data[1], slen)) goto sha2_error;
      if (!SHA224_Final((unsigned char *)sha, &sha_ctx_224)) goto sha2_error;

      rlen = SHA224_DIGEST_LENGTH;
      break;

    case 384:
      SHA512_CTX sha_ctx_384;
      if (!SHA384_Init(&sha_ctx_384)) goto sha2_error;
      if (!SHA384_Update(&sha_ctx_384, op_data[1], slen)) goto sha2_error;
      if (!SHA384_Final((unsigned char *)sha, &sha_ctx_384)) goto sha2_error;

      rlen = SHA384_DIGEST_LENGTH;
      break;

    case 512:
      SHA512_CTX sha_ctx_512;
      if (!SHA512_Init(&sha_ctx_512)) goto sha2_error;
      if (!SHA512_Update(&sha_ctx_512, op_data[1], slen)) goto sha2_error;
      if (!SHA512_Final((unsigned char *)sha, &sha_ctx_512)) goto sha2_error;

      rlen = SHA512_DIGEST_LENGTH;
      break;

    default:
      ExRaiseSqlError(heap, diags, EXE_BAD_ARG_TO_MATH_FUNC);
      *(*diags) << DgString0("SHA2");
      return ex_expr::EXPR_ERROR;
  }
  str_pad(op_data[0], rlen, ' ');

  char tmp[3];
  for (int i = 0; i < rlen; i++) {
    tmp[0] = tmp[1] = tmp[2] = '0';
    sprintf(tmp, "%.2x", (int)sha[i]);
    str_cpy_all(op_data[0] + i * 2, tmp, 2);
  }

  return ex_expr::EXPR_OK;
sha2_error:
  ExRaiseFunctionSqlError(heap, diags, EXE_INTERNAL_ERROR, derivedFunction(), origFunctionOperType());
  return ex_expr::EXPR_ERROR;
}

ex_expr::exp_return_type ExFunctionSha::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  unsigned char sha[SHA_DIGEST_LENGTH + 1] = {0};

  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);
  int slen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);
  int rlen = resultAttr->getLength();
  str_pad(op_data[0], rlen, ' ');

  SHA_CTX sha_ctx;

  SHA1_Init(&sha_ctx);
  SHA1_Update(&sha_ctx, op_data[1], slen);
  SHA1_Final((unsigned char *)sha, &sha_ctx);
  char tmp[3];
  for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
    tmp[0] = tmp[1] = tmp[2] = '0';
    sprintf(tmp, "%.2x", (int)sha[i]);
    str_cpy_all(op_data[0] + i * 2, tmp, 2);
  }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionMd5::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  unsigned char md5[17] = {0};

  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  int slen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);
  int rlen = resultAttr->getLength();
  if (slen == 0) {
    // move null value to result
    setNullValue(0, getOperand(0), op_data[-2 * MAX_OPERANDS]);
    return ex_expr::EXPR_NULL;
  }

  str_pad(op_data[0], rlen, ' ');
  MD5_CTX md5_ctx;

  MD5_Init(&md5_ctx);
  MD5_Update(&md5_ctx, op_data[1], slen);
  MD5_Final((unsigned char *)md5, &md5_ctx);

  char tmp[3];
  for (int i = 0; i < 16; i++) {
    tmp[0] = tmp[1] = tmp[2] = '0';
    sprintf(tmp, "%.2x", (int)md5[i]);
    str_cpy_all(op_data[0] + i * 2, tmp, 2);
  }

  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(32, op_data[-MAX_OPERANDS]);
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionIsIP::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diags) {
  char *resultData = op_data[0];
  char *srcData = op_data[1];
  Int16 i = 0, j = 0, p = 0;
  Attributes *resultAttr = getOperand(0);
  Attributes *srcAttr = getOperand(1);

  int slen = srcAttr->getLength(op_data[-MAX_OPERANDS + 1]);
  int rlen = resultAttr->getLength();

  if (getOperType() == ITM_ISIPV4) {
    if (string2ipv4(srcData, slen, NULL) == 0) {
      *(Int16 *)op_data[0] = 0;
      return ex_expr::EXPR_OK;
    } else {
      *(Int16 *)op_data[0] = 1;
      return ex_expr::EXPR_OK;
    }

  } else {
    Int16 gapcounter = 0, portidx = 0;
    ;
    char portion[IPV6_PARTS_NUM][MAX_IPV6_STRING_LEN + 1];
    char trimdata[MAX_IPV6_STRING_LEN + 1];
    str_pad(trimdata, MAX_IPV6_STRING_LEN + 1, 0);

    if (slen < MIN_IPV6_STRING_LEN) {
      *(Int16 *)op_data[0] = 0;
      return ex_expr::EXPR_OK;
    }

    char *ptr = srcData;

    // cannot start with single :
    if (*ptr == ':') {
      if (*(ptr + 1) != ':') {
        *(Int16 *)op_data[0] = 0;
        return ex_expr::EXPR_OK;
      }
    } else if (*ptr == ' ') {
      while (*ptr == ' ') ptr++;
    }

    char *start = ptr;
    if (slen - (srcData - ptr) > MAX_IPV6_STRING_LEN)  // must be padding space
    {
      if (start[MAX_IPV6_STRING_LEN] != ' ') {
        *(Int16 *)op_data[0] = 0;
        return ex_expr::EXPR_OK;
      } else {
        for (j = MAX_IPV6_STRING_LEN; j >= 0; j--) {
          if (ptr[j] != ' ')  // stop, j is the last non-space char
            break;
        }
        str_cpy_all(trimdata, start, j);
        start = trimdata;
      }
    }

    char ipv4[MAX_IPV6_STRING_LEN + 1];
    j = 0;
    int ipv4idx = 0;
    // try to split the string into portions delieted by ':'
    // also check '::', call it gap, there is only up to 1 gap
    // if there is a gap, portion number can be smaller than 8
    // without gap, portion number should be 8
    // each portion must be 16 bit integer in HEX format
    // leading 0 can be omit
    for (i = 0; i < slen; i++) {
      if (start[i] == ':') {
        portion[portidx][j] = 0;  // set the terminator

        if (start[i + 1] == ':') {
          if (j != 0)  // some characters are already saved into current portion
            portidx++;
          gapcounter++;
          j = 0;  // reset temp buffer pointer
          i++;
          continue;
        } else {
          // new portion start
          if (j == 0) {
            *(Int16 *)op_data[0] = 0;
            return ex_expr::EXPR_OK;
          }
          portidx++;
          j = 0;
          continue;
        }
      } else if (start[i] == '.')  // ipv4 mixed format
      {
        if (ipv4idx > 0) {
          *(Int16 *)op_data[0] = 0;
          return ex_expr::EXPR_OK;
        }

        str_cpy_all(ipv4, portion[portidx], str_len(portion[portidx]));
        if (strlen(start + i) < MAX_IPV4_STRING_LEN)  // 15 is the maximum IPV4 string length
          str_cat((const char *)ipv4, start + i, ipv4);
        else {
          *(Int16 *)op_data[0] = 0;
          return ex_expr::EXPR_OK;
        }

        if (string2ipv4(ipv4, strlen(ipv4), NULL) == 0) {
          *(Int16 *)op_data[0] = 0;
          return ex_expr::EXPR_OK;
        } else {
          ipv4idx = 2;  // ipv4 use 2 portions, 32 bits
          break;        // ipv4 string must be the last portion
        }
      }

      portion[portidx][j] = start[i];
      j++;
    }

    if (gapcounter > 1 || portidx > IPV6_PARTS_NUM - 1) {
      *(Int16 *)op_data[0] = 0;
      return ex_expr::EXPR_OK;
    } else if (gapcounter == 0 && portidx + ipv4idx < IPV6_PARTS_NUM - 1) {
      *(Int16 *)op_data[0] = 0;
      return ex_expr::EXPR_OK;
    }

    // check each IPV6 portion
    for (i = 0; i < portidx; i++) {
      int len = strlen(portion[i]);
      if (len > 4)  // IPV4 portion can be longer than 4 chars
      {
        if (ipv4idx == 0 || ((ipv4idx == 2) && (i != portidx - 1)))  // no IPV4 portion, or this is not the IPV4 portion
        {
          *(Int16 *)op_data[0] = 0;
          return ex_expr::EXPR_OK;
        }
      }
      for (j = 0; j < len; j++) {
        if ((portion[i][j] >= 'A' && portion[i][j] <= 'F') || (portion[i][j] >= 'a' && portion[i][j] <= 'f') ||
            (portion[i][j] >= '0' && portion[i][j] <= '9'))  // good
          continue;
        else {
          *(Int16 *)op_data[0] = 0;
          return ex_expr::EXPR_OK;
        }
      }
    }
    // everything is good, this is IPV6
    *(Int16 *)op_data[0] = 1;
    return ex_expr::EXPR_OK;
  }
}

// Parse json errors
static void ExRaiseJSONError(CollHeap *heap, ComDiagsArea **diagsArea, JsonReturnType type) {
  switch (type) {
    case JSON_INVALID_TOKEN:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_TOKEN);
      break;
    case JSON_INVALID_VALUE:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_VALUE);
      break;
    case JSON_INVALID_STRING:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_STRING);
      break;
    case JSON_INVALID_ARRAY_START:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_ARRAY_START);
      break;
    case JSON_INVALID_ARRAY_NEXT:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_ARRAY_NEXT);
      break;
    case JSON_INVALID_OBJECT_START:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_OBJECT_START);
      break;
    case JSON_INVALID_OBJECT_LABEL:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_OBJECT_LABEL);
      break;
    case JSON_INVALID_OBJECT_NEXT:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_OBJECT_NEXT);
      break;
    case JSON_INVALID_OBJECT_COMMA:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_OBJECT_COMMA);
      break;
    case JSON_INVALID_END:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_END);
      break;
    case JSON_END_PREMATURELY:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_END_PREMATURELY);
      break;
    case JSON_INVALID_KEY_NULL:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_INVALID_KEY_NULL);
      break;
    case JSON_PATH_LANGUAGE_INVALID:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_PATH_LANGUAGE_INVALID);
      break;
    case JSON_PATH_RESULT_ONLY_ONE_ELEM_ALLOW:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_PATH_RESULT_ONLY_ONE_ELEM_ALLOW);
      break;
    case JSON_PATH_RESULT_NOT_SCALAR:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_PATH_RESULT_NOT_SCALAR);
      break;
    case JSON_PATH_RESULT_EMPTY:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_PATH_RESULT_EMPTY);
      break;
    case JSON_PATH_RESULT_NEED_ARRAY_WRAPPER:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_PATH_RESULT_NEED_ARRAY_WRAPPER);
      break;
    default:
      ExRaiseSqlError(heap, diagsArea, EXE_JSON_UNEXPECTED_ERROR);
      break;
  }
}
/*
 * SOUNDEX(str) returns a character string containing the phonetic
 * representation of the input string. It lets you compare words that
 * are spelled differently, but sound alike in English.
 * The phonetic representation is defined in "The Art of Computer Programming",
 * Volume 3: Sorting and Searching, by Donald E. Knuth, as follows:
 *
 *  1. Retain the first letter of the string and remove all other occurrences
 *  of the following letters: a, e, h, i, o, u, w, y.
 *
 *  2. Assign numbers to the remaining letters (after the first) as follows:
 *        b, f, p, v = 1
 *        c, g, j, k, q, s, x, z = 2
 *        d, t = 3
 *        l = 4
 *        m, n = 5
 *        r = 6
 *
 *  3. If two or more letters with the same number were adjacent in the original
 *  name (before step 1), or adjacent except for any intervening h and w, then
 *  omit all but the first.
 *
 *  4. Return the first four bytes padded with 0.
 * */
ex_expr::exp_return_type ExFunctionSoundex::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int previous = 0;
  int current = 0;

  char *srcStr = op_data[1];
  char *tgtStr = op_data[0];
  int srcLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  int tgtLen = getOperand(0)->getLength();

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  str_pad(tgtStr, tgtLen, '\0');

  tgtStr[0] = toupper(srcStr[0]);  // Retain the first letter, convert to capital anyway
  Int16 setLen = 1;                // The first character is set already

  for (int i = 1; i < srcLen; ++i) {
    char chr = toupper(srcStr[i]);
    switch (chr) {
      case 'A':
      case 'E':
      case 'H':
      case 'I':
      case 'O':
      case 'U':
      case 'W':
      case 'Y':
        current = 0;
        break;
      case 'B':
      case 'F':
      case 'P':
      case 'V':
        current = 1;
        break;
      case 'C':
      case 'G':
      case 'J':
      case 'K':
      case 'Q':
      case 'S':
      case 'X':
      case 'Z':
        current = 2;
        break;
      case 'D':
      case 'T':
        current = 3;
        break;
      case 'L':
        current = 4;
        break;
      case 'M':
      case 'N':
        current = 5;
        break;
      case 'R':
        current = 6;
        break;
      default:
        break;
    }

    if (current)  // Only non-zero valued letter shall ve retained, 0 will be discarded
    {
      if (previous != current) {
        str_itoa(current, &tgtStr[setLen]);
        setLen++;  // A new character is set in target
      }
    }

    previous = current;

    if (setLen == tgtLen)  // Don't overhit the target string
      break;
  }  // end of for loop

  if (setLen < tgtLen) str_pad(tgtStr + setLen, (tgtLen - setLen), '0');

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionAESEncrypt::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  CharInfo::CharSet cs = ((SimpleType *)getOperand(0))->getCharSet();
  Attributes *tgt = getOperand(0);

  int source_len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  char *source = op_data[1];

  int key_len = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  unsigned char *key = (unsigned char *)op_data[2];

  unsigned char *result = (unsigned char *)op_data[0];

  unsigned char rkey[EVP_MAX_KEY_LENGTH];

  int iv_len_need = ComEncryption::getInitVecLen(aes_mode);

  unsigned char *iv = NULL;
  if (iv_len_need) {
    if (args_num == 3) {
      int iv_len_input = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
      if (iv_len_input == 0 || iv_len_input < iv_len_need) {
        // the length of iv is too short
        ExRaiseSqlError(heap, diagsArea, EXE_AES_INVALID_IV);
        *(*diagsArea) << DgInt0(iv_len_input) << DgInt1(iv_len_need);
        return ex_expr::EXPR_ERROR;
      }
      iv = (unsigned char *)op_data[3];
    } else {
      // it does not have iv argument, but the algorithm need iv
      ExRaiseSqlError(heap, diagsArea, EXE_ERR_PARAMCOUNT_FOR_FUNC);
      *(*diagsArea) << DgString0("AES_ENCRYPT");
      return ex_expr::EXPR_ERROR;
    }
  } else {
    if (args_num == 3) {
      // the algorithm doesn't need iv, give a warning
      ExRaiseSqlWarning(heap, diagsArea, EXE_OPTION_IGNORED);
      *(*diagsArea) << DgString0("IV");
    }
  }

  aes_create_key(key, key_len, rkey, aes_mode);

  int rc = 0;
  int tgtLen = 0;
  rc = ComEncryption::encryptData(aes_mode, (unsigned char *)source, source_len, rkey, iv, result, tgtLen);
  if (rc) goto aes_encrypt_error;

  tgt->setVarLength(tgtLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;

aes_encrypt_error:

  ExRaiseSqlError(heap, diagsArea, EXE_OPENSSL_ERROR);
  *(*diagsArea) << DgString0("AES_ENCRYPT FUNCTION");

  return ex_expr::EXPR_ERROR;
}

ex_expr::exp_return_type ExFunctionAESDecrypt::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgt = getOperand(0);

  int source_len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  const unsigned char *source = (unsigned char *)op_data[1];

  int key_len = getOperand(2)->getLength(op_data[-MAX_OPERANDS + 2]);
  const unsigned char *key = (unsigned char *)op_data[2];

  int maxLength = getOperand(0)->getLength();
  unsigned char *result = (unsigned char *)op_data[0];

  unsigned char rkey[EVP_MAX_KEY_LENGTH] = {0};

  int iv_len_need = ComEncryption::getInitVecLen(aes_mode);

  unsigned char *iv = NULL;
  if (iv_len_need) {
    if (args_num == 3) {
      int iv_len_input = getOperand(3)->getLength(op_data[-MAX_OPERANDS + 3]);
      if (iv_len_input == 0 || iv_len_input < iv_len_need) {
        // the length of iv is too short
        ExRaiseSqlError(heap, diagsArea, EXE_AES_INVALID_IV);
        *(*diagsArea) << DgInt0(iv_len_input) << DgInt1(iv_len_need);
        return ex_expr::EXPR_ERROR;
      }
      iv = (unsigned char *)op_data[3];
    } else {
      // it does not have iv argument, but the algorithm need iv
      ExRaiseSqlError(heap, diagsArea, EXE_ERR_PARAMCOUNT_FOR_FUNC);
      *(*diagsArea) << DgString0("AES_DECRYPT");
      return ex_expr::EXPR_ERROR;
    }
  } else {
    if (args_num == 3) {
      // the algorithm doesn't need iv, give a warning
      ExRaiseSqlWarning(heap, diagsArea, EXE_OPTION_IGNORED);
      *(*diagsArea) << DgString0("IV");
    }
  }

  aes_create_key(key, key_len, rkey, aes_mode);

  int rc = 0;
  int tgtLen = 0;
  rc = ComEncryption::decryptData(aes_mode, (unsigned char *)source, source_len, rkey, iv, result, tgtLen);
  if (rc) goto aes_decrypt_error;

  tgt->setVarLength(tgtLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;

aes_decrypt_error:
  ExRaiseSqlError(heap, diagsArea, EXE_OPENSSL_ERROR);
  *(*diagsArea) << DgString0("AES_DECRYPT FUNCTION");

  return ex_expr::EXPR_ERROR;
}

void regexpReplaceCleanup(regex_t *reg, char *s1, char *s2, char *s3, char *s4, CollHeap *heap) {
  regfree(reg);
  NADELETEBASIC(s1, heap);
  NADELETEBASIC(s2, heap);
  NADELETEBASIC(s3, heap);
  NADELETEBASIC(s4, heap);
}

static int regexpReplaceBuildReplaceStr(char *repPattern, int repPatternLen, char *replaceStr, int &replaceStrLen,
                                        char *srcStr, regmatch_t *pm, const size_t nmatch, ComDiagsArea **diagsArea,
                                        CollHeap *heap) {
  NABoolean raiseError = FALSE;
  int val;
  int maxReplaceStrLen = replaceStrLen;
  char errCause[128];
  int dollarOffset = 0;
  int numLen = 0;
  int subExpLen = 0;
  int targetPos = 0;
  int sourcePos = 0;

  memset(errCause, '\0', sizeof(errCause));

  if (repPatternLen == 0) {
    replaceStrLen = repPatternLen;
    return 1;
  }
  if (nmatch > 99) {
    strncpy(&errCause[0], "Attempting to match more than 99 subexpressions", sizeof(errCause) - 1);
    goto raiseError;
  }

  char *dollarPtr;
  dollarPtr = strchr(repPattern, '$');
  dollarOffset = (int)(dollarPtr - repPattern);  // 0-based
  // keep going if this is an escaped $
  while (dollarPtr && (dollarOffset > 0) && (dollarPtr[-1] == '\\') && (dollarOffset + 1 < repPatternLen)) {
    dollarPtr = strchr(&repPattern[dollarOffset + 1], '$');
    dollarOffset = (int)(dollarPtr - repPattern);
  }
  while (dollarPtr &&
         // last char in repPattern is unescaped dollar
         (dollarOffset + sourcePos < repPatternLen - 1)) {
    numLen = 0;
    while (isdigit(dollarPtr[numLen + 1])) numLen++;
    val = str_atoi(&dollarPtr[1], numLen);
    if ((val <= 0) || (val > nmatch) || (pm[val].rm_so < 0)) {
      strncpy(&errCause[0], "Subexpression specified does not exist or has an index not in [1-99]",
              sizeof(errCause) - 1);
      goto raiseError;
    }

    subExpLen = pm[val].rm_eo - pm[val].rm_so;
    if (targetPos + dollarOffset > maxReplaceStrLen) {
      strncpy(&errCause[0], "Overflow while constructing replacement string", sizeof(errCause) - 1);
      goto raiseError;
    }
    str_cpy_all(&replaceStr[targetPos], &repPattern[sourcePos], dollarOffset);
    targetPos += dollarOffset;
    sourcePos += dollarOffset;
    if (targetPos + subExpLen > maxReplaceStrLen) {
      strncpy(&errCause[0], "Overflow while constructing replacement string", sizeof(errCause) - 1);
      goto raiseError;
    }
    str_cpy_all(&replaceStr[targetPos], &srcStr[pm[val].rm_so], subExpLen);
    targetPos += subExpLen;
    sourcePos += numLen + 1;

    dollarPtr = strchr(&repPattern[sourcePos], '$');
    dollarOffset = (int)(dollarPtr - repPattern - sourcePos);  // 0-based
    // keep going if this is an escaped $
    while (dollarPtr && (dollarOffset > 0) && (dollarPtr[-1] == '\\') &&
           (dollarOffset + sourcePos + 1 < repPatternLen)) {
      dollarPtr = strchr(&repPattern[dollarOffset + sourcePos + 1], '$');
      dollarOffset = (int)(dollarPtr - repPattern - sourcePos);
    }
  }
  if (targetPos + repPatternLen - sourcePos > maxReplaceStrLen) {
    strncpy(&errCause[0], "Overflow while constructing replacement string", sizeof(errCause) - 1);
    goto raiseError;
  }
  str_cpy_all(&replaceStr[targetPos], &repPattern[sourcePos], repPatternLen - sourcePos);
  targetPos += repPatternLen - sourcePos;
  replaceStrLen = targetPos;
  return 1;  // success

raiseError:
  ExRaiseSqlError(heap, diagsArea, (ExeErrorCode)8455);
  **diagsArea << DgString0(errCause);
  return 0;  // error
}

short ExFunctionBase64EncDec::decodeValue(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgt = getOperand(0);

  int source_len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  unsigned char *source = (unsigned char *)op_data[1];
  char *result = op_data[0];

  int retLen = -1;
  if (maxBuflen_ == -1)  // explicit type not specified
  {
    retLen = str_decode_base64(source, source_len, result, tgt->getLength());
    if ((retLen > 0) && (DFS2REC::isCharacterString(tgt->getDatatype()))) {
      int rc = 0;
      unsigned int outputLen = 0;
      char *firstUntranslated = NULL;

      // allocate buffer of max possible result length. This is where
      // result will be converted and validated for charset correctness.
      if (!decodedBuf_) {
        decodedBuf_ = new (heap) char[tgt->getLength()];
      }

      // If target is character, then decoded string
      // can only be in CharSet ISO88591, UTF8 or UCS2.
      // Try converting decoded string to locale UTF8 to tell if it is a
      // UTF8 compatible CharSet, otherwise it's CharSet UCS2.
      rc = UTF8ToLocale(cnv_version1, result, retLen, decodedBuf_, retLen, cnv_UTF8, firstUntranslated, &outputLen);
      if ((rc != 0 && tgt->getCharSet() == CharInfo::UTF8) || (rc == 0 && tgt->getCharSet() == CharInfo::UCS2)) {
        const char *csname = CharInfo::getCharSetName(tgt->getCharSet());
        ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
        *(*diagsArea) << DgString0(csname) << DgString1("source");
        retLen = -1;
      }
    }
  } else {
    if (!decodedBuf_) {
      decodedBuf_ = new (heap) char[maxBuflen_];
    }

    retLen = str_decode_base64(source, source_len, decodedBuf_, maxBuflen_);
    if (retLen > 0) {
      // explicitly specified datatype length must be the same as the
      // source operand length.
      if (retLen != tgt->getLength()) {
        ExRaiseSqlError(heap, diagsArea, EXE_STMT_NOT_SUPPORTED);

        char errBuf[500];
        str_sprintf(errBuf, "Decoded length(%d) must be equal to the length(%d) of specified target type.", retLen,
                    tgt->getLength());

        *(*diagsArea) << DgString0(errBuf);

        retLen = -1;
      } else
        str_cpy_all(result, decodedBuf_, retLen);
    }
  }

  return retLen;
}

ex_expr::exp_return_type ExFunctionBase64EncDec::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *tgt = getOperand(0);

  int source_len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  if (source_len == 0) {
    tgt->setVarLength(0, op_data[-MAX_OPERANDS]);

    return ex_expr::EXPR_OK;
  }

  unsigned char *source = (unsigned char *)op_data[1];
  char *result = op_data[0];
  int retLen = -1;

  if (isEncode_) {
    retLen = str_encode_base64(source, source_len, result, tgt->getLength());
  } else {
    retLen = decodeValue(op_data, heap, diagsArea);
  }

  if (retLen <= 0) {
    char hexstr[MAX_OFFENDING_SOURCE_DATA_DISPLAY_LEN];
    ExRaiseSqlError(heap, diagsArea, EXE_ENCODE_DECODE_ERROR);
    char *errStr = stringToHex(hexstr, sizeof(hexstr), (char *)source, source_len);
    *(*diagsArea) << DgString0((isEncode_ ? "encoded" : "decoded")) << DgString1(errStr);

    return ex_expr::EXPR_ERROR;
  }

  tgt->setVarLength(retLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

static short computeNextKeyValue(NAString &keyValue, Int16 getBytesPerChar) {
  ComASSERT(getBytesPerChar == 1);

  for (size_t i = keyValue.length(); i--; i) {
    unsigned char c = keyValue[i];
    if (c < UCHAR_MAX) {
      keyValue[i] = ++c;  // NOT keyValue[i]++: NAString is signed char
      break;
    }
    keyValue.remove(i);
  }

  return keyValue.length();
}

static short computeNextKeyValue_UTF8(NAString &keyValue, Int16 getBytesPerChar) {
  ComASSERT(getBytesPerChar == 4);

  for (size_t i = keyValue.length(); i--; i) {
    unsigned char c = keyValue[i];
    if ((c & 0xC0) == 0x80)  // If not first byte in a char,
      continue;              // keep going back by one byte at a time

    unsigned int UCS4val = 0;  // LocaleCharToUCS4 requires "unsigned int"
    int charLenIn = LocaleCharToUCS4(&keyValue[i], 4, &UCS4val, cnv_UTF8);
    if ((charLenIn > 0) && (UCS4val < 0x1FFFFF))  // Less than max char ?
    {
      char tmpBuf[10];
      UCS4val++;
      int charLenOut = UCS4ToLocaleChar(&UCS4val, tmpBuf, 10, cnv_UTF8);
      tmpBuf[charLenOut] = '\0';
      //
      // Replace character with next character
      //
      keyValue.remove(i);
      keyValue.insert(i, tmpBuf);
      break;
    } else
      keyValue.remove(i);
  }

  return keyValue.length();
}

static short computeNextKeyValue(NAWString &keyValue, Int16 getBytesPerChar, NAWchar maxValue) {
  ComASSERT(getBytesPerChar == SQL_DBCHAR_SIZE);

  for (size_t i = keyValue.length(); i--; i) {
    NAWchar c = keyValue[i];

#ifdef NA_LITTLE_ENDIAN
#ifdef IS_MP
    if (CharInfo::is_NCHAR_MP(getCharSet())) wc_swap_bytes(&c, 1);
#endif
#endif

    if (c < maxValue) {
      c += 1;
#ifdef NA_LITTLE_ENDIAN
#ifdef IS_MP
      if (CharInfo::is_NCHAR_MP(getCharSet())) wc_swap_bytes(&c, 1);
#endif
#endif
      keyValue[i] = c;  // NOT keyValue[i]++: NAWString is signed char
      break;
    }
    keyValue.remove(i);
  }

  return keyValue.length();
}

ex_function_beginkey::~ex_function_beginkey() {}

ex_expr::exp_return_type ex_function_beginkey::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *target = getOperand(0);
  char *result = op_data[0];
  const char *underscoreChar;
  const char *percentChar;
  UInt16 underscoreChar_len = BYTES_PER_NAWCHAR;
  UInt16 percentChar_len = BYTES_PER_NAWCHAR;
  int max_len = getOperand(0)->getLength();
  int source_len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  ComASSERT(max_len >= source_len);

  char *source = (char *)op_data[1];

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  if (cs == CharInfo::UNICODE) {
    underscoreChar = (char *)L"_", percentChar = (char *)L"%";
  } else if (cs == CharInfo::ISO88591 || cs == CharInfo::UTF8) {
    underscoreChar = (char *)"_", underscoreChar_len = 1;
    percentChar = (char *)"%";
    percentChar_len = 1;
  }

  LikePatternString likePatternString(source, source_len, cs, NULL, NULL, underscoreChar, underscoreChar_len,
                                      percentChar, percentChar_len);

  LikePattern pattern(likePatternString, heap, cs);

  if (pattern.error()) {
    ExRaiseSqlError(heap, diagsArea, pattern.error());
    return ex_expr::EXPR_ERROR;
  }

  // such as  '___as' should not in
  if (pattern.getClauseLength() > 0 && pattern.getType() != LikePatternStringIterator::UNDERSCORE) {
    NAString patternString(pattern.getPattern(), pattern.getClauseLength(), heap);
    str_cpy_all(result, (char *)patternString.data(), patternString.length());
    target->setVarLength(patternString.length(), op_data[-MAX_OPERANDS]);
  } else {
    // handle _utf8'', _ucs2''. this will return a empty string
    if (pattern.getType() == LikePatternStringIterator::NON_WILDCARD)
      str_cpy_all(result, pattern.getPattern(), pattern.getClauseLength()),
          target->setVarLength(pattern.getClauseLength(), op_data[-MAX_OPERANDS]);
    // for mantis 15166, if begins with wildcard char, then report an error
    else {
      ExRaiseSqlError(heap, diagsArea, EXE_STMT_NOT_SUPPORTED);

      char errBuf[100];
      str_sprintf(errBuf, "pattern string in dynamic parameter begin with %% or _ is not supported");
      *(*diagsArea) << DgString0(errBuf);
      return ex_expr::EXPR_ERROR;
    }
  }
  return ex_expr::EXPR_OK;
}

ex_function_endkey::~ex_function_endkey() {}

ex_expr::exp_return_type ex_function_endkey::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *targetAttr = getOperand(0);
  char *result = op_data[0];
  const char *underscoreChar;
  const char *percentChar;
  UInt16 underscoreChar_len = BYTES_PER_NAWCHAR;
  UInt16 percentChar_len = BYTES_PER_NAWCHAR;
  int max_len = getOperand(0)->getLength();
  int source_len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);
  ComASSERT(max_len >= source_len);

  char *source = (char *)op_data[1];

  CharInfo::CharSet cs = ((SimpleType *)getOperand(1))->getCharSet();

  if (cs == CharInfo::UNICODE) {
    underscoreChar = (char *)L"_";
    percentChar = (char *)L"%";
  } else if (cs == CharInfo::ISO88591 || cs == CharInfo::UTF8) {
    underscoreChar = (char *)"_";
    underscoreChar_len = 1;
    percentChar = (char *)"%";
    percentChar_len = 1;
  }

  LikePatternString likePatternString(source, source_len, cs, NULL, NULL, underscoreChar, underscoreChar_len,
                                      percentChar, percentChar_len);

  LikePattern pattern(likePatternString, heap, cs);

  if (pattern.error()) {
    ExRaiseSqlError(heap, diagsArea, pattern.error());
    return ex_expr::EXPR_ERROR;
  }

  // such as '___as' should not in
  if (pattern.getClauseLength() > 0 && pattern.getType() != LikePatternStringIterator::UNDERSCORE) {
    if (cs == CharInfo::UCS2) {
      NAWString prefixW((NAWchar *)pattern.getPattern(), pattern.getClauseLength() >> 1);
      computeNextKeyValue(prefixW, bytesPerChar_, maxValue_);
      str_cpy_all(result, (char *)prefixW.data(), prefixW.length() << 1);
      targetAttr->setVarLength(prefixW.length() << 1, op_data[-MAX_OPERANDS]);
    } else if (cs == CharInfo::ISO88591) {
      NAString prefix(pattern.getPattern(), pattern.getClauseLength(), heap);
      computeNextKeyValue(prefix, bytesPerChar_);
      str_cpy_all(result, prefix.data(), prefix.length());
      targetAttr->setVarLength(prefix.length(), op_data[-MAX_OPERANDS]);
    } else if (cs == CharInfo::UTF8) {
      NAString prefix(pattern.getPattern(), pattern.getClauseLength(), heap);
      computeNextKeyValue_UTF8(prefix, bytesPerChar_);
      str_cpy_all(result, prefix.data(), prefix.length());
      targetAttr->setVarLength(prefix.length(), op_data[-MAX_OPERANDS]);
    } else {
      const char *csname = CharInfo::getCharSetName(cs);
      ExRaiseSqlError(heap, diagsArea, EXE_INVALID_CHARACTER);
      *(*diagsArea) << DgString0(csname) << DgString1("ENDKEY");
      return ex_expr::EXPR_ERROR;
    }
  } else {
    if (pattern.getType() == LikePatternStringIterator::PERCENT)
      str_cpy_all(result, percentChar, percentChar_len),
          targetAttr->setVarLength(percentChar_len, op_data[-MAX_OPERANDS]);
    else if (pattern.getType() == LikePatternStringIterator::UNDERSCORE)
      str_cpy_all(result, underscoreChar, underscoreChar_len),
          targetAttr->setVarLength(underscoreChar_len, op_data[-MAX_OPERANDS]);
    // handle _utf8'', _ucs2''. this will return a empty string
    else if (pattern.getType() == LikePatternStringIterator::NON_WILDCARD)
      str_cpy_all(result, pattern.getPattern(), pattern.getClauseLength()),
          targetAttr->setVarLength(pattern.getClauseLength(), op_data[-MAX_OPERANDS]);
    // do not handle there
  }
  return ex_expr::EXPR_OK;
}
