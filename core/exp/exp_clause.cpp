
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

#include <stdio.h>
#include <stdlib.h>

#include "ExpBitMuxFunction.h"
#include "ExpComposite.h"
#include "ExpSequenceFunction.h"
#include "common/Platform.h"
#include "common/wstr.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_datetime.h"
#include "exp/exp_stdh.h"
#include "exp/exp_function.h"
#include "exp_math_func.h"
#include "ttime.h"

extern int compExprNum;

static const char *ZERO_LENGTH_TIMESTAMP = " 00:00:00.000000";

#define GenAssert(p, msg)              \
  if (!(p)) {                          \
    NAAssert(msg, __FILE__, __LINE__); \
  };

void setVCLength(char *VCLen, int VCLenSize, int value);

///////////////////////////////////////////////////////////
// class ex_clause
///////////////////////////////////////////////////////////
void ex_clause::copyOperands(ex_clause *clause, Space *space) {
  NABoolean showplan = (clause->getOperand() ? clause->getOperand(0)->showplan() : FALSE);

  int numOperands = ((showplan) ? (2 * clause->getNumOperands()) : clause->getNumOperands());

  op_ = (AttributesPtr *)(space->allocateAlignedSpace(numOperands * sizeof(AttributesPtr)));

  for (int i = 0; i < numOperands; i++) {
    Attributes *attrOld = clause->getOperand(i);
    Attributes *attrNew = NULL;

    if (attrOld != NULL) {
      int size = attrOld->getClassSize();
      attrNew = (Attributes *)new (space) char[size];
      memcpy((char *)attrNew, (char *)attrOld, size);
    }

    op_[i] = attrNew;
  }

  switch (clause->getType()) {
    case ex_clause::INOUT_TYPE: {
      char *temp;
      int len;
      ex_inout_clause *inout = (ex_inout_clause *)this;

      // Copy strings associated with this class.  The strings being with a
      // a length count of size long, followed by the string itself.  Also,
      // remember that new() will automatically allocated an aligned space, so
      // nothing additional needs to be done here.

      if (inout->getHeading()) {
        len = sizeof(int) + *((int *)inout->getHeading());
        temp = new (space) char[len];
        memcpy(temp, inout->getHeading(), len);
        inout->setHeading(temp);
      }

      if (inout->getName()) {
        len = sizeof(int) + *((int *)inout->getName());
        temp = new (space) char[len];
        memcpy(temp, inout->getName(), len);
        inout->setName(temp);
      }

      if (inout->getTableName()) {
        len = sizeof(int) + *((int *)inout->getTableName());
        temp = new (space) char[len];
        memcpy(temp, inout->getTableName(), len);
        inout->setTableName(temp);
      }

      if (inout->getSchemaName()) {
        len = sizeof(int) + *((int *)inout->getSchemaName());
        temp = new (space) char[len];
        memcpy(temp, inout->getSchemaName(), len);
        inout->setSchemaName(temp);
      }

      if (inout->getCatalogName()) {
        len = sizeof(int) + *((int *)inout->getCatalogName());
        temp = new (space) char[len];
        memcpy(temp, inout->getCatalogName(), len);
        inout->setCatalogName(temp);
      }

      break;
    }

    case ex_clause::FUNCTION_TYPE:
      if (clause->getClassID() == ex_clause::FUNC_RAISE_ERROR_ID) {
        ExpRaiseErrorFunction *func = (ExpRaiseErrorFunction *)this;
        const char *cName = func->getConstraintName();
        const char *tName = func->getTableName();

        // Set contraint name
        if (cName) {
          int len = strlen(cName);
          char *temp = space->allocateAndCopyToAlignedSpace(cName, len, 0);
          func->setConstraintName(temp);
        }

        // Set table name
        if (tName) {
          int len = strlen(tName);
          char *temp = space->allocateAndCopyToAlignedSpace(tName, len, 0);
          func->setTableName(temp);
        }
      }

      break;
  }
}

ex_clause::ex_clause(clause_type type, OperatorTypeEnum oper_type, short num_operands, Attributes **op, Space *space)
    : NAVersionedObject(type), nextClause_(NULL), op_(NULL) {
  clauseType_ = type;
  operType_ = oper_type;
  numOperands_ = num_operands;
  pciLink_ = NULL;
  nextClause_ = (ExClausePtr)NULL;
  flags_ = 0;
  //  instruction_   = -1;
  instrArrayIndex_ = -1;

  str_pad(fillers_, sizeof(fillers_), '\0');

  // Further qualify these types...
  //
  if ((type == ex_clause::FUNCTION_TYPE) || (type == ex_clause::LIKE_TYPE) || (type == ex_clause::MATH_FUNCTION_TYPE) ||
      (type == ex_clause::AGGREGATE_TYPE)) {
    switch (oper_type) {
      case ITM_LIKE:
        setClassID(LIKE_CLAUSE_CHAR_ID);
        break;
      case ITM_LIKE_DOUBLEBYTE:
        setClassID(LIKE_CLAUSE_DOUBLEBYTE_ID);
        break;
      case ITM_REGEXP:
        setClassID(REGEXP_CLAUSE_CHAR_ID);
        break;
      case ITM_ASCII:
      case ITM_CODE_VALUE:
      case ITM_UNICODE_CODE_VALUE:
      case ITM_NCHAR_MP_CODE_VALUE:
        setClassID(FUNC_ASCII_ID);
        break;
      case ITM_UNICODE_MAXBYTES:
      case ITM_UTF8_MAXBYTES:
      case ITM_NCHAR_MP_MAXBYTES:
      case ITM_MAXBYTES:
        setClassID(FUNC_MAXBYTES_ID);
        break;
      case ITM_CHAR:
      case ITM_NCHAR_MP_CHAR:
      case ITM_UNICODE_CHAR:
        setClassID(FUNC_CHAR_ID);
        break;
      case ITM_CHAR_LENGTH:
        setClassID(FUNC_CHAR_LEN_ID);
        break;
      case ITM_CHAR_LENGTH_DOUBLEBYTE:
        setClassID(FUNC_CHAR_LEN_DOUBLEBYTE_ID);
        break;
      case ITM_CONVERTFROMHEX:
      case ITM_CONVERTTOHEX:
        setClassID(FUNC_CVT_HEX_ID);
        break;
      case ITM_OCTET_LENGTH:
        setClassID(FUNC_OCT_LEN_ID);
        break;
      case ITM_POSITION:
        setClassID(FUNC_POSITION_ID);
        break;
      case ITM_SPLIT_PART:
        setClassID(FUNC_SPLIT_PART_ID);
        break;
      case ITM_CONCAT:
        setClassID(FUNC_CONCAT_ID);
        break;
      case ITM_REPEAT:
        setClassID(FUNC_REPEAT_ID);
        break;
      case ITM_REPLACE:
        setClassID(FUNC_REPLACE_ID);
        break;
      case ITM_REGEXP_REPLACE:
        setClassID(REGEXP_REPLACE);
        break;
      case ITM_REGEXP_SUBSTR:
      case ITM_REGEXP_COUNT:
        setClassID(FUNC_REGEXP_SUBSTR_COUNT);
        break;
      case ITM_SUBSTR:
        setClassID(FUNC_SUBSTR_ID);
        break;
      case ITM_SUBSTR_DOUBLEBYTE:
        setClassID(FUNC_SUBSTR_DOUBLEBYTE_ID);
        break;
      case ITM_TRIM:
        setClassID(FUNC_TRIM_ID);
        break;
      case ITM_TRANSLATE:
        setClassID(FUNC_TRANSLATE_ID);
        break;
      case ITM_TRIM_DOUBLEBYTE:
        setClassID(FUNC_TRIM_DOUBLEBYTE_ID);
        break;
      case ITM_LOWER:
        setClassID(FUNC_LOWER_ID);
        break;
      case ITM_UPPER:
        setClassID(FUNC_UPPER_ID);
        break;
      case ITM_UPPER_UNICODE:
        setClassID(FUNC_UPPER_UNICODE_ID);
        break;
      case ITM_LOWER_UNICODE:
        setClassID(FUNC_LOWER_UNICODE_ID);
        break;
      case ITM_UNIX_TIMESTAMP:
        setClassID(FUNC_UNIX_TIMESTAMP_ID);
        break;
      case ITM_SLEEP:
        setClassID(FUNC_SLEEP_ID);
        break;
      case ITM_CURRENT_TIMESTAMP:
        setClassID(FUNC_CURRENT_TIMESTAMP_ID);
        break;
      case ITM_CURRENT_TIMESTAMP_RUNNING:
        setClassID(FUNC_CURRENT_TIMESTAMP_ID);
        break;
      case ITM_COMP_ENCODE:
      case ITM_COMP_DECODE:
        setClassID(FUNC_ENCODE_ID);
        break;
      case ITM_EXPLODE_VARCHAR:
        setClassID(FUNC_EXPLODE_VARCHAR_ID);
        break;
      case ITM_HASH:
        setClassID(FUNC_HASH_ID);
        break;
      case ITM_HIVE_HASH:
        setClassID(FUNC_HIVEHASH_ID);
        break;
      case ITM_HASHCOMB:
        setClassID(FUNC_HASHCOMB_ID);
        break;
      case ITM_HIVE_HASHCOMB:
        setClassID(FUNC_HIVEHASHCOMB_ID);
        break;
      case ITM_HDPHASH:
        setClassID(FUNC_HDPHASH_ID);
        break;
      case ITM_HDPHASHCOMB:
        setClassID(FUNC_HDPHASHCOMB_ID);
        break;
      case ITM_BITMUX:
        setClassID(FUNC_BITMUX_ID);
        break;
      case ITM_REPLACE_NULL:
        setClassID(FUNC_REPLACE_NULL_ID);
        break;
      case ITM_MOD:
        setClassID(FUNC_MOD_ID);
        break;
      case ITM_MASK_SET:
      case ITM_MASK_CLEAR:
        setClassID(FUNC_MASK_ID);
        break;
      case ITM_SHIFT_RIGHT:
      case ITM_SHIFT_LEFT:
        setClassID(FUNC_SHIFT_ID);
        break;
      case ITM_ABS:
        setClassID(FUNC_ABS_ID);
        break;
      case ITM_RETURN_TRUE:
      case ITM_RETURN_FALSE:
      case ITM_RETURN_NULL:
        setClassID(FUNC_BOOL_ID);
        break;
      case ITM_CONVERTTIMESTAMP:
        setClassID(FUNC_CONVERTTIMESTAMP_ID);
        break;
      case ITM_DATEFORMAT:
        setClassID(FUNC_DATEFORMAT_ID);
        break;
      case ITM_NUMBERFORMAT:
        setClassID(FUNC_NUMBERFORMAT_ID);
        break;
      case ITM_DAYOFWEEK:
        setClassID(FUNC_DAYOFWEEK_ID);
        break;
      case ITM_EXTRACT:
      case ITM_EXTRACT_ODBC:
        setClassID(FUNC_EXTRACT_ID);
        break;
      case ITM_MONTHS_BETWEEN:
        setClassID(FUNC_MONTHSBETWEEN_ID);
        break;
      case ITM_JULIANTIMESTAMP:
        setClassID(FUNC_JULIANTIMESTAMP_ID);
        break;
      case ITM_EXEC_COUNT:
        setClassID(FUNC_EXEC_COUNT_ID);
        break;
      case ITM_CURR_TRANSID:
        setClassID(FUNC_CURR_TRANSID_ID);
        break;
      case ITM_SHA1:
        setClassID(FUNC_SHA1_ID);
        break;
      case ITM_SHA2_224:
      case ITM_SHA2_256:
      case ITM_SHA2_384:
      case ITM_SHA2_512:
        setClassID(FUNC_SHA2_ID);
        break;
      case ITM_MD5:
        setClassID(FUNC_MD5_ID);
        break;
      case ITM_CRC32:
        setClassID(FUNC_CRC32_ID);
        break;
      case ITM_ISIPV4:
      case ITM_ISIPV6:
        setClassID(FUNC_ISIP_ID);
        break;
      case ITM_INET_ATON:
        setClassID(FUNC_INETATON_ID);
        break;
      case ITM_INET_NTOA:
        setClassID(FUNC_INETNTOA_ID);
        break;
      case ITM_USER:
      case ITM_USERID:
      case ITM_AUTHTYPE:
      case ITM_AUTHNAME:
        setClassID(FUNC_USER_ID);
        break;
      case ITM_CURRENT_USER:
      case ITM_SESSION_USER:
        setClassID(FUNC_ANSI_USER_ID);
        break;
      case ITM_CURRENT_TENANT:
        setClassID(FUNC_ANSI_TENANT_ID);
        break;
      case ITM_STDDEV_SAMP:
        setClassID(FUNC_STDDEV_SAMP_ID);
        break;
      case ITM_STDDEV_POP:
        setClassID(FUNC_STDDEV_POP_ID);
        break;
      case ITM_VARIANCE_SAMP:
        setClassID(FUNC_VARIANCE_SAMP_ID);
        break;
      case ITM_VARIANCE_POP:
        setClassID(FUNC_VARIANCE_POP_ID);
        break;
      case ITM_RAISE_ERROR:
        setClassID(FUNC_RAISE_ERROR_ID);
        break;
      case ITM_RANDOMNUM:
        setClassID(FUNC_RANDOMNUM_ID);
        break;
      case ITM_RAND_SELECTION:
        setClassID(FUNC_RAND_SELECTION_ID);
        break;
      case ITM_PROGDISTRIB:
        setClassID(FUNC_PROGDISTRIB_ID);
        break;
      case ITM_PROGDISTRIBKEY:
        setClassID(FUNC_PROGDISTKEY_ID);
        break;
      case ITM_PAGROUP:
        setClassID(FUNC_PAGROUP_ID);
        break;
      case ITM_HASH2_DISTRIB:
        setClassID(FUNC_HASH2_DISTRIB_ID);
        break;
      case ITM_UNPACKCOL:
        setClassID(FUNC_UNPACKCOL_ID);
        break;
      case ITM_PACK_FUNC:
        setClassID(FUNC_PACK_ID);
        break;
      case ITM_ROWSETARRAY_SCAN:
        setClassID(FUNC_ROWSETARRAY_SCAN_ID);
        break;
      case ITM_ROWSETARRAY_ROWID:
        setClassID(FUNC_ROWSETARRAY_ROW_ID);
        break;
      case ITM_ROWSETARRAY_INTO:
        setClassID(FUNC_ROWSETARRAY_INTO_ID);
        break;
      case ITM_RANGE_LOOKUP:
        setClassID(FUNC_RANGE_LOOKUP_ID);
        break;
      case ITM_OFFSET:
        setClassID(FUNC_OFFSET_ID);
        break;
      case ITM_DEGREES:
      case ITM_PI:
      case ITM_RADIANS:
      case ITM_ROUND:
      case ITM_SCALE_TRUNC:
      case ITM_ACOS:
      case ITM_ASIN:
      case ITM_ATAN:
      case ITM_ATAN2:
      case ITM_CEIL:
      case ITM_COS:
      case ITM_COSH:
      case ITM_EXP:
      case ITM_FLOOR:
      case ITM_LOG:
      case ITM_LOG10:
      case ITM_LOG2:
      case ITM_SIN:
      case ITM_SINH:
      case ITM_SQRT:
      case ITM_TAN:
      case ITM_TANH:
      case ITM_COT:
      case ITM_EXPONENT:
      case ITM_POWER:
        setClassID(FUNC_MATH_ID);
        break;
      case ITM_BITAND:
      case ITM_BITOR:
      case ITM_BITXOR:
      case ITM_BITNOT:
      case ITM_BITEXTRACT:
      case ITM_CONVERTTOBITS:
        setClassID(FUNC_BIT_OPER_ID);
        break;
      case ITM_ONE_ROW:
        setClassID(AGGR_ONE_ROW_ID);
        break;
      case ITM_ANY_TRUE_MAX:
        setClassID(AGGR_ANY_TRUE_MAX_ID);
        break;
      case ITM_AGGR_MIN_MAX:
        setClassID(AGGR_MIN_MAX_ID);
        break;
      case ITM_AGGR_GROUPING_FUNC:
        setClassID(AGGR_GROUPING_ID);
        break;
      case ITM_CURRENTEPOCH:
      case ITM_VSBBROWTYPE:
      case ITM_VSBBROWCOUNT:
        setClassID(FUNC_GENERICUPDATEOUTPUT_ID);
        break;
      case ITM_INTERNALTIMESTAMP:
        setClassID(FUNC_INTERNALTIMESTAMP_ID);
        break;
      case ITM_UNIQUE_EXECUTE_ID:
        setClassID(FUNC_UNIQUE_EXECUTE_ID_ID);
        break;
      case ITM_GET_TRIGGERS_STATUS:
        setClassID(FUNC_GET_TRIGGERS_STATUS_ID);
        break;
      case ITM_GET_BIT_VALUE_AT:
        setClassID(FUNC_GET_BIT_VALUE_AT_ID);
        break;
      case ITM_IS_BITWISE_AND_TRUE:
        setClassID(FUNC_IS_BITWISE_AND_TRUE);
        break;
      case ITM_NULLIFZERO:
        setClassID(FUNC_NULLIFZERO);
        break;
      case ITM_NVL:
        setClassID(FUNC_NVL);
        break;

      // for ngram
      case ITM_FIRSTNGRAM:
        setClassID(FUNC_FIRSTNGRAM);
        break;
      case ITM_NGRAMCOUNT:
        setClassID(FUNC_NGRAMCOUNT);
        break;

      case ITM_QUERYID_EXTRACT:
        setClassID(FUNC_QUERYID_EXTRACT);
        break;
      case ITM_UNIQUE_ID:
      case ITM_UNIQUE_ID_SYS_GUID:
      case ITM_UNIQUE_SHORT_ID:
        setClassID(FUNC_UNIQUE_ID);
        break;
      case ITM_ROWNUM:
        setClassID(FUNC_ROWNUM);
        break;
      case ITM_HBASE_COLUMN_LOOKUP:
        setClassID(FUNC_HBASE_COLUMN_LOOKUP);
        break;
      case ITM_HBASE_COLUMNS_DISPLAY:
        setClassID(FUNC_HBASE_COLUMNS_DISPLAY);
        break;
      case ITM_HBASE_COLUMN_CREATE:
        setClassID(FUNC_HBASE_COLUMN_CREATE);
        break;
      case ITM_TOKENSTR:
        setClassID(FUNC_TOKENSTR_ID);
        break;
      case ITM_REVERSE:
        setClassID(FUNC_REVERSE_ID);
        break;
      case ITM_CAST_TYPE:
        setClassID(FUNC_CAST_TYPE);
        break;
      case ITM_SEQUENCE_VALUE:
        setClassID(FUNC_SEQUENCE_VALUE);
        break;
      case ITM_PIVOT_GROUP:
        setClassID(FUNC_PIVOT_GROUP);
        break;
      case ITM_HEADER:
        setClassID(FUNC_HEADER);
        break;
      case ITM_HBASE_VISIBILITY:
        setClassID(FUNC_HBASE_VISIBILITY);
        break;
      case ITM_HBASE_VISIBILITY_SET:
        setClassID(FUNC_HBASE_VISIBILITY_SET);
        break;
      case ITM_HBASE_TIMESTAMP:
        setClassID(FUNC_HBASE_TIMESTAMP);
        break;
      case ITM_HBASE_ROWID:
        setClassID(FUNC_HBASE_ROWID);
        break;
      case ITM_HBASE_VERSION:
        setClassID(FUNC_HBASE_VERSION);
        break;
      case ITM_SOUNDEX:
        setClassID(FUNC_SOUNDEX_ID);
        break;
      case ITM_AES_ENCRYPT:
        setClassID(FUNC_AES_ENCRYPT);
        break;
      case ITM_AES_DECRYPT:
        setClassID(FUNC_AES_DECRYPT);
        break;
      case ITM_RANGE_VALUES_MERGE:
        setClassID(FUNC_RANGE_VALUES_MERGE_ID);
        break;
      case ITM_RANGE_VALUES_INSERT:
        setClassID(FUNC_RANGE_VALUES_INSERT_ID);
        break;
      case ITM_RANGE_VALUES_IN:
        setClassID(FUNC_RANGE_VALUES_IN_ID);
        break;
      case ITM_RANGE_VALUES_COPY:
        setClassID(FUNC_RANGE_VALUES_COPY_ID);
        break;
      case ITM_RANGE_VALUES_UNPACK:
        setClassID(FUNC_RANGE_VALUES_UNPACK_ID);
        break;
      case ITM_COMPOSITE_CONCAT:
        setClassID(FUNC_COMPOSITE_CONCAT);
        break;
      case ITM_COMPOSITE_CREATE:
        setClassID(FUNC_COMPOSITE_CREATE);
        break;
      case ITM_COMPOSITE_DISPLAY:
        setClassID(FUNC_COMPOSITE_DISPLAY);
        break;
      case ITM_COMPOSITE_EXTRACT:
        setClassID(FUNC_COMPOSITE_EXTRACT);
        break;
      case ITM_COMPOSITE_ARRAY_CAST:
        setClassID(FUNC_COMPOSITE_ARRAY_CAST);
        break;
      case ITM_COMPOSITE_ARRAY_LENGTH:
        setClassID(FUNC_COMPOSITE_ARRAY_LENGTH);
        break;
      case ITM_COMPOSITE_HIVE_CAST:
        setClassID(FUNC_COMPOSITE_HIVE_CAST);
        break;
      case ITM_ENCODE_BASE64:
      case ITM_DECODE_BASE64:
        setClassID(FUNC_BASE64_ENC_DEC);
        break;
      case ITM_BEGINKEY:
        setClassID(FUNC_BEGINKEY);
        break;
      case ITM_ENDKEY:
        setClassID(FUNC_ENDKEY);
        break;
      default:
        GenAssert(0, "ex_clause: Unknown Class ID.");
        break;
    }
  }

  clauseNum_ = 0;
  numberBranchTargets_ = 0;

  /* Make sure that all operands have valid values for atp, atp_index
     and offset.                                                      */
  if (op) {
    short numOperands = (op[0]->showplan() ? num_operands * 2 : num_operands);

    if (space)

      op_ = (AttributesPtr *)(space->allocateAlignedSpace(numOperands * sizeof(AttributesPtr)));

    else
      //      op_ = (AttributesPtr *)(new char[numOperands * sizeof(AttributesPtr)]);
      GenAssert(0, "Internal Error: must pass the space pointer.");

    int i = 0;
    Attributes *attr = NULL;
    for (i = 0; i < num_operands; i++) {
      if (!op[i]) continue;

      if ((op[i]->getAtp() < 0) || (op[i]->getAtpIndex() < 0) ||
          (op[i]->getTupleFormat() == ExpTupleDesc::UNINITIALIZED_FORMAT))
        GenAssert(0, "Internal Error: Operand attributes are not valid.");

      if (space)
        attr = op[i]->newCopy(space);
      else
        //	op_[i] = op[i]->newCopy();
        GenAssert(0, "Internal Error: must pass the space pointer.");

      attr->setAtp(op[i]->getAtp());
      attr->setAtpIndex(op[i]->getAtpIndex());
      attr->setOffset(op[i]->getOffset());
      attr->setRelOffset(op[i]->getRelOffset());
      attr->setVoaOffset(op[i]->getVoaOffset());
      attr->setNullBitIndex(op[i]->getNullBitIndex());
      attr->setNextFieldIndex(op[i]->getNextFieldIndex());
      attr->setTupleFormat(op[i]->getTupleFormat());
      attr->setDefaultFieldNum(op[i]->getDefaultFieldNum());

      if (attr->getNullFlag()) {
        if (i == 0)
          flags_ |= ANY_OUTPUT_NULLABLE;
        else
          flags_ |= ANY_INPUT_NULLABLE;
      }
      op_[i] = attr;
    }

    if (op_[0]->showplan()) {
      for (i = num_operands; i < numOperands; i++) {
        if (space)
          op_[i] = op[i]->newCopy(space);
        else
          op_[i] = op[i]->newCopy();
      }
    }
  } else
    op_ = 0;
};

ex_clause::~ex_clause() {}

// -----------------------------------------------------------------------
// This method returns the virtual function table pointer for an object
// with the given class ID; used by NAVersionedObject::driveUnpack().
// -----------------------------------------------------------------------
char *ex_clause::findVTblPtr(short classID) {
  char *vtblPtr;
  switch (classID) {
    case ex_clause::COMP_TYPE:
      GetVTblPtr(vtblPtr, ex_comp_clause);
      break;
    case ex_clause::CONV_TYPE:
      GetVTblPtr(vtblPtr, ex_conv_clause);
      break;
    case ex_clause::UN_LOGIC_TYPE:
      GetVTblPtr(vtblPtr, ex_unlogic_clause);
      break;
    case ex_clause::ARITH_TYPE:
      GetVTblPtr(vtblPtr, ex_arith_clause);
      break;
    case ex_clause::ARITH_SUM_TYPE:
      GetVTblPtr(vtblPtr, ex_arith_sum_clause);
      break;
    case ex_clause::ARITH_COUNT_TYPE:
      GetVTblPtr(vtblPtr, ex_arith_count_clause);
      break;
    case ex_clause::LIKE_CLAUSE_CHAR_ID:
      GetVTblPtr(vtblPtr, ex_like_clause_char);
      break;
    case ex_clause::LIKE_CLAUSE_DOUBLEBYTE_ID:
      GetVTblPtr(vtblPtr, ex_like_clause_doublebyte);
      break;
    case ex_clause::REGEXP_CLAUSE_CHAR_ID:
      GetVTblPtr(vtblPtr, ExRegexpClauseChar);
      break;
    case ex_clause::REGEXP_REPLACE:
      GetVTblPtr(vtblPtr, ExRegexpReplace);
      break;
    case ex_clause::FUNC_REGEXP_SUBSTR_COUNT:
      GetVTblPtr(vtblPtr, ExRegexpSubstrOrCount);
      break;
    case ex_clause::FUNC_ASCII_ID:
      GetVTblPtr(vtblPtr, ExFunctionAscii);
      break;
    case ex_clause::FUNC_MAXBYTES_ID:
      GetVTblPtr(vtblPtr, ExFunctionMaxBytes);
      break;
    case ex_clause::FUNC_CHAR_ID:
      GetVTblPtr(vtblPtr, ExFunctionChar);
      break;
    case ex_clause::FUNC_CHAR_LEN_ID:
      GetVTblPtr(vtblPtr, ex_function_char_length);
      break;
    case ex_clause::FUNC_CHAR_LEN_DOUBLEBYTE_ID:
      GetVTblPtr(vtblPtr, ex_function_char_length_doublebyte);
      break;
    case ex_clause::FUNC_CVT_HEX_ID:
      GetVTblPtr(vtblPtr, ExFunctionConvertHex);
      break;
    case ex_clause::FUNC_OCT_LEN_ID:
      GetVTblPtr(vtblPtr, ex_function_oct_length);
      break;
    case ex_clause::FUNC_POSITION_ID:
      GetVTblPtr(vtblPtr, ex_function_position);
      break;
    case ex_clause::FUNC_SPLIT_PART_ID:
      GetVTblPtr(vtblPtr, ex_function_split_part);
      break;
    case ex_clause::FUNC_CONCAT_ID:
      GetVTblPtr(vtblPtr, ex_function_concat);
      break;
    case ex_clause::FUNC_REPEAT_ID:
      GetVTblPtr(vtblPtr, ExFunctionRepeat);
      break;
    case ex_clause::FUNC_REPLACE_ID:
      GetVTblPtr(vtblPtr, ExFunctionReplace);
      break;
    case ex_clause::FUNC_SUBSTR_ID:
      GetVTblPtr(vtblPtr, ex_function_substring);
      break;
    // 12/23/97: added for unicode
    case ex_clause::FUNC_SUBSTR_DOUBLEBYTE_ID:
      GetVTblPtr(vtblPtr, ex_function_substring_doublebyte);
      break;
    case ex_clause::FUNC_TRIM_ID:
      GetVTblPtr(vtblPtr, ex_function_trim_char);
      break;
    case ex_clause::FUNC_TRANSLATE_ID:
      GetVTblPtr(vtblPtr, ex_function_translate);
      break;
    // 12/29/97: added for unicode
    case ex_clause::FUNC_TRIM_DOUBLEBYTE_ID:
      GetVTblPtr(vtblPtr, ex_function_trim_doublebyte);
      break;
    case ex_clause::FUNC_LOWER_ID:
      GetVTblPtr(vtblPtr, ex_function_lower);
      break;
    case ex_clause::FUNC_UPPER_ID:
      GetVTblPtr(vtblPtr, ex_function_upper);
      break;
    // 12/17/97: added for unicode UPPER()
    case ex_clause::FUNC_UPPER_UNICODE_ID:
      GetVTblPtr(vtblPtr, ex_function_upper_unicode);
      break;
    // 12/17/97: added for unicode LOWER()
    case ex_clause::FUNC_LOWER_UNICODE_ID:
      GetVTblPtr(vtblPtr, ex_function_lower_unicode);
      break;
    case ex_clause::FUNC_SLEEP_ID:
      GetVTblPtr(vtblPtr, ex_function_sleep);
      break;
    case ex_clause::FUNC_UNIX_TIMESTAMP_ID:
      GetVTblPtr(vtblPtr, ex_function_unixtime);
      break;
    case ex_clause::FUNC_CURRENT_TIMESTAMP_ID:
      GetVTblPtr(vtblPtr, ex_function_current);
      break;
    case ex_clause::FUNC_INTERNALTIMESTAMP_ID:
      GetVTblPtr(vtblPtr, ExFunctionInternalTimestamp);
      break;
    case ex_clause::FUNC_GENERICUPDATEOUTPUT_ID:
      GetVTblPtr(vtblPtr, ExFunctionGenericUpdateOutput);
      break;
    case ex_clause::FUNC_UNIQUE_EXECUTE_ID_ID:
      GetVTblPtr(vtblPtr, ex_function_unique_execute_id);
      break;
    case ex_clause::FUNC_GET_TRIGGERS_STATUS_ID:
      GetVTblPtr(vtblPtr, ex_function_get_triggers_status);
      break;
    case ex_clause::FUNC_GET_BIT_VALUE_AT_ID:
      GetVTblPtr(vtblPtr, ex_function_get_bit_value_at);
      break;
    case ex_clause::FUNC_IS_BITWISE_AND_TRUE:
      GetVTblPtr(vtblPtr, ex_function_is_bitwise_and_true);
      break;
    case ex_clause::FUNC_ENCODE_ID:
      GetVTblPtr(vtblPtr, ex_function_encode);
      break;
    case ex_clause::FUNC_EXPLODE_VARCHAR_ID:
      GetVTblPtr(vtblPtr, ex_function_explode_varchar);
      break;
    case ex_clause::FUNC_HASH_ID:
      GetVTblPtr(vtblPtr, ex_function_hash);
      break;
    case ex_clause::FUNC_HASHCOMB_ID:
      GetVTblPtr(vtblPtr, ExHashComb);
      break;
    case ex_clause::FUNC_HDPHASH_ID:
      GetVTblPtr(vtblPtr, ExHDPHash);
      break;
    case ex_clause::FUNC_HDPHASHCOMB_ID:
      GetVTblPtr(vtblPtr, ExHDPHashComb);
      break;
    case ex_clause::FUNC_BITMUX_ID:
      GetVTblPtr(vtblPtr, ExpBitMuxFunction);
      break;
    case ex_clause::FUNC_REPLACE_NULL_ID:
      GetVTblPtr(vtblPtr, ex_function_replace_null);
      break;
    case ex_clause::FUNC_MOD_ID:
      GetVTblPtr(vtblPtr, ex_function_mod);
      break;
    case ex_clause::FUNC_MASK_ID:
      GetVTblPtr(vtblPtr, ex_function_mask);
      break;
    case ex_clause::FUNC_SHIFT_ID:
      GetVTblPtr(vtblPtr, ExFunctionShift);
      break;
    case ex_clause::FUNC_ABS_ID:
      GetVTblPtr(vtblPtr, ex_function_abs);
      break;
    case ex_clause::FUNC_BOOL_ID:
      GetVTblPtr(vtblPtr, ex_function_bool);
      break;
    case ex_clause::FUNC_CONVERTTIMESTAMP_ID:
      GetVTblPtr(vtblPtr, ex_function_converttimestamp);
      break;
    case ex_clause::FUNC_DATEFORMAT_ID:
      GetVTblPtr(vtblPtr, ex_function_dateformat);
      break;
    case ex_clause::FUNC_NUMBERFORMAT_ID:
      GetVTblPtr(vtblPtr, ex_function_numberformat);
      break;
    case ex_clause::FUNC_DAYOFWEEK_ID:
      GetVTblPtr(vtblPtr, ex_function_dayofweek);
      break;
    case ex_clause::FUNC_EXTRACT_ID:
      GetVTblPtr(vtblPtr, ex_function_extract);
      break;
    case ex_clause::FUNC_MONTHSBETWEEN_ID:
      GetVTblPtr(vtblPtr, ex_function_monthsbetween);
      break;
    case ex_clause::FUNC_JULIANTIMESTAMP_ID:
      GetVTblPtr(vtblPtr, ex_function_juliantimestamp);
      break;
    case ex_clause::FUNC_EXEC_COUNT_ID:
      GetVTblPtr(vtblPtr, ex_function_exec_count);
      break;
    case ex_clause::FUNC_CURR_TRANSID_ID:
      GetVTblPtr(vtblPtr, ex_function_curr_transid);
      break;
    case ex_clause::FUNC_USER_ID:
      GetVTblPtr(vtblPtr, ex_function_user);
      break;
    case ex_clause::FUNC_ANSI_USER_ID:
      GetVTblPtr(vtblPtr, ex_function_ansi_user);
      break;
    case ex_clause::FUNC_ANSI_TENANT_ID:
      GetVTblPtr(vtblPtr, ex_function_ansi_tenant);
      break;
    case ex_clause::FUNC_VARIANCE_SAMP_ID:
      GetVTblPtr(vtblPtr, ExFunctionSVariance);
      break;
    case ex_clause::FUNC_VARIANCE_POP_ID:
      GetVTblPtr(vtblPtr, ExFunctionSVariance);
      break;
    case ex_clause::FUNC_STDDEV_SAMP_ID:
      GetVTblPtr(vtblPtr, ExFunctionSStddev);
      break;
    case ex_clause::FUNC_STDDEV_POP_ID:
      GetVTblPtr(vtblPtr, ExFunctionSStddev);
      break;
    case ex_clause::FUNC_RAISE_ERROR_ID:
      GetVTblPtr(vtblPtr, ExpRaiseErrorFunction);
      break;
    case ex_clause::FUNC_RANDOMNUM_ID:
      GetVTblPtr(vtblPtr, ExFunctionRandomNum);
      break;
    case ex_clause::FUNC_RAND_SELECTION_ID:
      GetVTblPtr(vtblPtr, ExFunctionRandomSelection);
      break;
    case ex_clause::FUNC_PROGDISTRIB_ID:
      GetVTblPtr(vtblPtr, ExProgDistrib);
      break;
    case ex_clause::FUNC_PROGDISTKEY_ID:
      GetVTblPtr(vtblPtr, ExProgDistribKey);
      break;
    case ex_clause::FUNC_PAGROUP_ID:
      GetVTblPtr(vtblPtr, ExPAGroup);
      break;
    case ex_clause::FUNC_HASH2_DISTRIB_ID:
      GetVTblPtr(vtblPtr, ExHash2Distrib);
      break;
    case ex_clause::FUNC_HEADER:
      GetVTblPtr(vtblPtr, ExHeaderClause);
      break;
    case ex_clause::FUNC_UNPACKCOL_ID:
      GetVTblPtr(vtblPtr, ExUnPackCol);
      break;
    case ex_clause::FUNC_PACK_ID:
      GetVTblPtr(vtblPtr, ExFunctionPack);
      break;
    case ex_clause::FUNC_RANGE_LOOKUP_ID:
      GetVTblPtr(vtblPtr, ExFunctionRangeLookup);
      break;
    case ex_clause::FUNC_OFFSET_ID:
      GetVTblPtr(vtblPtr, ExpSequenceFunction);
      break;
    case ex_clause::FUNCTION_TYPE:
      GetVTblPtr(vtblPtr, ex_function_clause);
      break;
    case ex_clause::FUNC_MATH_ID:
      GetVTblPtr(vtblPtr, ExFunctionMath);
      break;
    case ex_clause::FUNC_BIT_OPER_ID:
      GetVTblPtr(vtblPtr, ExFunctionBitOper);
      break;
    case ex_clause::BOOL_RESULT_TYPE:
      GetVTblPtr(vtblPtr, bool_result_clause);
      break;
    case ex_clause::BOOL_TYPE:
      GetVTblPtr(vtblPtr, ex_bool_clause);
      break;
    case ex_clause::BRANCH_TYPE:
      GetVTblPtr(vtblPtr, ex_branch_clause);
      break;
    case ex_clause::INOUT_TYPE:
      GetVTblPtr(vtblPtr, ex_inout_clause);
      break;
    case ex_clause::NOOP_TYPE:
      GetVTblPtr(vtblPtr, ex_noop_clause);
      break;
    case ex_clause::AGGR_ONE_ROW_ID:
      GetVTblPtr(vtblPtr, ex_aggr_one_row_clause);
      break;
    case ex_clause::AGGR_ANY_TRUE_MAX_ID:
      GetVTblPtr(vtblPtr, ex_aggr_any_true_max_clause);
      break;
    case ex_clause::AGGR_MIN_MAX_ID:
      GetVTblPtr(vtblPtr, ex_aggr_min_max_clause);
      break;
    case ex_clause::AGGR_GROUPING_ID:
      GetVTblPtr(vtblPtr, ExFunctionGrouping);
      break;
    case ex_clause::AGGREGATE_TYPE:
      GetVTblPtr(vtblPtr, ex_aggregate_clause);
      break;
    case ex_clause::FUNC_ROWSETARRAY_SCAN_ID:
      GetVTblPtr(vtblPtr, ExRowsetArrayScan) break;
    case ex_clause::FUNC_ROWSETARRAY_ROW_ID:
      GetVTblPtr(vtblPtr, ExRowsetArrayRowid);
      break;
    case ex_clause::FUNC_ROWSETARRAY_INTO_ID:
      GetVTblPtr(vtblPtr, ExRowsetArrayInto);
      break;
    case ex_clause::FUNC_NULLIFZERO:
      GetVTblPtr(vtblPtr, ex_function_nullifzero);
      break;
    case ex_clause::FUNC_NVL:
      GetVTblPtr(vtblPtr, ex_function_nvl);
      break;

    case ex_clause::FUNC_FIRSTNGRAM:
      GetVTblPtr(vtblPtr, ex_function_firstngram);
      break;
    case ex_clause::FUNC_NGRAMCOUNT:
      GetVTblPtr(vtblPtr, ex_function_ngramcount);
      break;
    case ex_clause::FUNC_QUERYID_EXTRACT:
      GetVTblPtr(vtblPtr, ex_function_queryid_extract);
      break;
    case ex_clause::FUNC_TOKENSTR_ID:
      GetVTblPtr(vtblPtr, ExFunctionTokenStr);
      break;
    case ex_clause::FUNC_REVERSE_ID:
      GetVTblPtr(vtblPtr, ExFunctionReverseStr);
      break;

    case ex_clause::FUNC_HIVEHASH_ID:
      GetVTblPtr(vtblPtr, ex_function_hivehash);
      break;
    case ex_clause::FUNC_HIVEHASHCOMB_ID:
      GetVTblPtr(vtblPtr, ExHiveHashComb);
      break;
    case ex_clause::FUNC_UNIQUE_ID:
      GetVTblPtr(vtblPtr, ExFunctionUniqueId);
      break;
    case ex_clause::FUNC_ROWNUM:
      GetVTblPtr(vtblPtr, ExFunctionRowNum);
      break;
    case ex_clause::FUNC_HBASE_COLUMN_LOOKUP:
      GetVTblPtr(vtblPtr, ExFunctionHbaseColumnLookup);
      break;
    case ex_clause::FUNC_HBASE_COLUMNS_DISPLAY:
      GetVTblPtr(vtblPtr, ExFunctionHbaseColumnsDisplay);
      break;
    case ex_clause::FUNC_HBASE_COLUMN_CREATE:
      GetVTblPtr(vtblPtr, ExFunctionHbaseColumnCreate);
      break;
    case ex_clause::FUNC_CAST_TYPE:
      GetVTblPtr(vtblPtr, ExFunctionCastType);
      break;
    case ex_clause::FUNC_SEQUENCE_VALUE:
      GetVTblPtr(vtblPtr, ExFunctionSequenceValue);
      break;
    case ex_clause::FUNC_PIVOT_GROUP:
      GetVTblPtr(vtblPtr, ex_pivot_group_clause);
      break;
    case ex_clause::FUNC_HBASE_TIMESTAMP:
      GetVTblPtr(vtblPtr, ExFunctionHbaseTimestamp);
      break;
    case ex_clause::FUNC_HBASE_VISIBILITY:
      GetVTblPtr(vtblPtr, ExFunctionHbaseVisibility);
      break;
    case ex_clause::FUNC_HBASE_VISIBILITY_SET:
      GetVTblPtr(vtblPtr, ExFunctionHbaseVisibilitySet);
      break;
    case ex_clause::FUNC_HBASE_VERSION:
      GetVTblPtr(vtblPtr, ExFunctionHbaseVersion);
      break;
    case ex_clause::FUNC_HBASE_ROWID:
      GetVTblPtr(vtblPtr, ExFunctionHbaseRowid);
      break;
    case ex_clause::FUNC_SHA1_ID:
      GetVTblPtr(vtblPtr, ExFunctionSha);
      break;
    case ex_clause::FUNC_SHA2_ID:
      GetVTblPtr(vtblPtr, ExFunctionSha2);
      break;
    case ex_clause::FUNC_MD5_ID:
      GetVTblPtr(vtblPtr, ExFunctionMd5);
      break;
    case ex_clause::FUNC_CRC32_ID:
      GetVTblPtr(vtblPtr, ExFunctionCrc32);
      break;
    case ex_clause::FUNC_ISIP_ID:
      GetVTblPtr(vtblPtr, ExFunctionIsIP);
      break;
    case ex_clause::FUNC_INETATON_ID:
      GetVTblPtr(vtblPtr, ExFunctionInetAton);
      break;
    case ex_clause::FUNC_INETNTOA_ID:
      GetVTblPtr(vtblPtr, ExFunctionInetNtoa);
      break;
    case ex_clause::FUNC_SOUNDEX_ID:
      GetVTblPtr(vtblPtr, ExFunctionSoundex);
      break;
    case ex_clause::FUNC_AES_ENCRYPT:
      GetVTblPtr(vtblPtr, ExFunctionAESEncrypt);
      break;
    case ex_clause::FUNC_AES_DECRYPT:
      GetVTblPtr(vtblPtr, ExFunctionAESDecrypt);
      break;
    case ex_clause::FUNC_RANGE_VALUES_MERGE_ID:
    case ex_clause::FUNC_RANGE_VALUES_INSERT_ID:
    case ex_clause::FUNC_RANGE_VALUES_IN_ID:
    case ex_clause::FUNC_RANGE_VALUES_COPY_ID:
    case ex_clause::FUNC_RANGE_VALUES_UNPACK_ID:
      GetVTblPtr(vtblPtr, ExFunctionRangeOfValues);
      break;
    case ex_clause::FUNC_COMPOSITE_CONCAT:
      GetVTblPtr(vtblPtr, ExpCompositeConcat);
      break;
    case ex_clause::FUNC_COMPOSITE_CREATE:
      GetVTblPtr(vtblPtr, ExpCompositeCreate);
      break;
    case ex_clause::FUNC_COMPOSITE_DISPLAY:
      GetVTblPtr(vtblPtr, ExpCompositeDisplay);
      break;
    case ex_clause::FUNC_COMPOSITE_EXTRACT:
      GetVTblPtr(vtblPtr, ExpCompositeExtract);
      break;
    case ex_clause::FUNC_COMPOSITE_ARRAY_CAST:
      GetVTblPtr(vtblPtr, ExpCompositeArrayCast);
      break;
    case ex_clause::FUNC_COMPOSITE_ARRAY_LENGTH:
      GetVTblPtr(vtblPtr, ExpCompositeArrayLength);
      break;
    case ex_clause::FUNC_COMPOSITE_HIVE_CAST:
      GetVTblPtr(vtblPtr, ExpCompositeHiveCast);
      break;
    case ex_clause::FUNC_BASE64_ENC_DEC:
      GetVTblPtr(vtblPtr, ExFunctionBase64EncDec);
      break;
    case ex_clause::FUNC_BEGINKEY:
      GetVTblPtr(vtblPtr, ex_function_beginkey);
      break;
    case ex_clause::FUNC_ENDKEY:
      GetVTblPtr(vtblPtr, ex_function_endkey);
      break;
    default:
      GetVTblPtr(vtblPtr, ex_clause);
      break;
  }
  return vtblPtr;
}

ex_expr::exp_return_type ex_clause::processNulls(char *null_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  for (short i = 1; i < getNumOperands(); i++) {
    // if value is missing, then it is a null value.
    // Move it to result and return.
    if ((getOperand(i)->getNullFlag()) &&  // nullable
        (!null_data[i]))                   // missing value
    {
      // This test is only needed in derived ex_conv_clause::processNulls;
      // (! get_operand(0)->getNullFlag()) should be impossible here.
      //
      // if (! get_operand(0)->getNullFlag()) // target not nullable
      //   {
      //     // Attempt to put NULL into a column with a
      //     // NOT NULL NONDROPPABLE constraint.
      //     // ## Need to supply name of constraint and name of table here.
      //     ExRaiseSqlError(heap, diagsArea, EXE_TABLE_CHECK_CONSTRAINT);
      //     return ex_expr::EXPR_ERROR;
      //   }

      // move null value to result.
      ExpTupleDesc::setNullValue(null_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
      return ex_expr::EXPR_NULL;
    }
  }

  // move 0 to the null bytes of result
  if (getOperand(0)->getNullFlag()) {
    ExpTupleDesc::clearNullValue(null_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }

  return ex_expr::EXPR_OK;
}

Long ex_clause::packClause(void *space, int /*size*/) {
  if (op_) {
    if (op_[0]->showplan()) {
      for (int i = numOperands_; i < 2 * numOperands_; i++) op_[i].pack(space);
    }
    for (int i = 0; i < numOperands_; i++) op_[i].pack(space);
  }
  op_.packShallow(space);
  return NAVersionedObject::pack(space);
}

Long ex_clause::pack(void *space) { return packClause(space, sizeof(ex_clause)); }

int ex_clause::unpackClause(void *base, void *reallocator) {
  if (op_) {
    if (op_.unpackShallow(base)) return -1;
    for (int i = 0; i < numOperands_; i++) {
      if (op_[i].unpack(base, reallocator)) return -1;
    }
    if (op_[0]->showplan()) {
      for (int i = numOperands_; i < 2 * numOperands_; i++) {
        if (op_[i].unpack(base, reallocator)) return -1;
      }
    }
  }
  return NAVersionedObject::unpack(base, reallocator);
}

int ex_clause::unpack(void *base, void *reallocator) { return unpackClause(base, reallocator); }

const char *getOperTypeEnumAsString(Int16 /*OperatorTypeEnum*/ ote) {
  switch (ote) {
    // Note, this list is arranged in the same order as the types
    // appear in common/OperTypeEnum.h, please keep the same order
    // when adding new types
    case ITM_AND:
      return "ITM_AND";
    case ITM_OR:
      return "ITM_OR";

    // unary logic operators
    case ITM_NOT:
      return "ITM_NOT";
    case ITM_IS_TRUE:
      return "ITM_IS_TRUE";
    case ITM_IS_FALSE:
      return "ITM_IS_FALSE";
    case ITM_IS_NULL:
      return "ITM_IS_NULL";
    case ITM_IS_NOT_NULL:
      return "ITM_IS_NOT_NULL";
    case ITM_IS_UNKNOWN:
      return "ITM_IS_UNKNOWN";
    case ITM_IS_NOT_UNKNOWN:
      return "ITM_IS_NOT_UNKNOWN";

    // binary comparison operators
    case ITM_EQUAL:
      return "ITM_EQUAL";
    case ITM_NOT_EQUAL:
      return "ITM_NOT_EQUAL";
    case ITM_LESS:
      return "ITM_LESS";
    case ITM_LESS_EQ:
      return "ITM_LESS_EQ";
    case ITM_GREATER:
      return "ITM_GREATER";
    case ITM_GREATER_EQ:
      return "ITM_GREATER_EQ";

    // unary arithmetic operators
    case ITM_NEGATE:
      return "ITM_NEGATE";
    case ITM_INVERSE:
      return "ITM_INVERSE";
    // binary arithmetic operators
    case ITM_PLUS:
      return "ITM_PLUS";
    case ITM_MINUS:
      return "ITM_MINUS";
    case ITM_TIMES:
      return "ITM_TIMES";
    case ITM_DIVIDE:
      return "ITM_DIVIDE";
    case ITM_EXPONENT:
      return "ITM_EXPONENT";

    // aggregate functions
    case ITM_AVG:
      return "ITM_AVG";
    case ITM_MAX:
      return "ITM_MAX";
    case ITM_MIN:
      return "ITM_MIN";
    case ITM_MAX_ORDERED:
      return "ITM_MAX_ORDERED";
    case ITM_MIN_ORDERED:
      return "ITM_MIN_ORDERED";
    case ITM_SUM:
      return "ITM_SUM";
    case ITM_COUNT:
      return "ITM_COUNT";
    case ITM_COUNT_NONULL:
      return "ITM_COUNT_NONULL";
    case ITM_STDDEV_SAMP:
      return "ITM_STDDEV_SAMP";
    case ITM_STDDEV_POP:
      return "ITM_STDDEV_POP";
    case ITM_VARIANCE_SAMP:
      return "ITM_VARIANCE_SAMP";
    case ITM_VARIANCE_POP:
      return "ITM_VARIANCE_POP";
    case ITM_BASECOL:
      return "ITM_BASECOL";

    case ITM_ONE_ROW:
      return "ITM_ONE_ROW";
    case ITM_ONEROW:
      return "ITM_ONEROW";
    case ITM_ONE_TRUE:
      return "ITM_ONE_TRUE";
    case ITM_ANY_TRUE:
      return "ITM_ANY_TRUE";
    case ITM_ANY_TRUE_MAX:
      return "ITM_ANY_TRUE_MAX";
    case ITM_MAX_INCL_NULL:
      return "ITM_MAX_INCL_NULL";

    case ITM_PIVOT_GROUP:
      return "ITM_PIVOT_GROUP";

    case ITM_ORC_MAX_NV:
      return "ITM_ORC_MAX_NV";
    case ITM_ORC_SUM_NV:
      return "ITM_ORC_SUM_NV";

    case ITM_AGGR_MIN_MAX:
      return "ITM_AGGR_MIN_MAX";
    case ITM_AGGR_GROUPING_FUNC:
      return "ITM_AGGR_GROUPING_FUNC";

    // custom functions
    case ITM_USER_DEF_FUNCTION:
      return "ITM_USER_DEF_FUNCTION";
    case ITM_BETWEEN:
      return "ITM_BETWEEN";
    case ITM_LIKE:
      return "ITM_LIKE";
    case ITM_REGEXP:
      return "ITM_REGEXP";
    case ITM_UNIX_TIMESTAMP:
      return "ITM_UNIX_TIMESTAMP";
    case ITM_SLEEP:
      return "ITM_SLEEP";
    case ITM_CURRENT_TIMESTAMP:
      return "ITM_CURRENT_TIMESTAMP";
    case ITM_CURRENT_USER:
      return "ITM_CURRENT_USER";
    case ITM_SESSION_USER:
      return "ITM_SESSION_USER";
    case ITM_USER:
      return "ITM_USER";
    case ITM_CURRENT_TENANT:
      return "ITM_CURRENT_TENANT";
    case ITM_CURRENT_CATALOG:
      return "ITM_CURRENT_CATALOG";
    case ITM_CURRENT_SCHEMA:
      return "ITM_CURRENT_SCHEMA";
    case ITM_AUTHNAME:
      return "ITM_AUTHNAME";
    case ITM_AUTHTYPE:
      return "ITM_AUTHTYPE";

    case ITM_BOOL_RESULT:
      return "ITM_BOOL_RESULT";
    case ITM_NO_OP:
      return "ITM_NO_OP";

    case ITM_CASE:
      return "ITM_CASE";
    case ITM_IF_THEN_ELSE:
      return "ITM_IF_THEN_ELSE";
    case ITM_RETURN_TRUE:
      return "ITM_RETURN_TRUE";
    case ITM_RETURN_FALSE:
      return "ITM_RETURN_FALSE";
    case ITM_RETURN_NULL:
      return "ITM_RETURN_NULL";
    case ITM_COMP_ENCODE:
      return "ITM_COMP_ENCODE";
    case ITM_COMP_DECODE:
      return "ITM_COMP_DECODE";
    case ITM_HASH:
      return "ITM_HASH";
    case ITM_REPLACE_NULL:
      return "ITM_REPLACE_NULL";
    case ITM_PACK_FUNC:
      return "ITM_PACK_FUNC";
    case ITM_BITMUX:
      return "ITM_BITMUX";
    case ITM_OVERLAPS:
      return "ITM_OVERLAPS";
    case ITM_RAISE_ERROR:
      return "ITM_RAISE_ERROR";

    case ITM_USERID:
      return "ITM_USERID";

    // sequence functions
    case ITM_DIFF1:
      return "ITM_DIFF1";
    case ITM_DIFF2:
      return "ITM_DIFF2";
    case ITM_LAST_NOT_NULL:
      return "ITM_LAST_NOT_NULL";
    case ITM_MOVING_COUNT:
      return "ITM_MOVING_COUNT";
    case ITM_MOVING_SUM:
      return "ITM_MOVING_SUM";
    case ITM_MOVING_AVG:
      return "ITM_MOVING_AVG";
    case ITM_MOVING_MAX:
      return "ITM_MOVING_MAX";
    case ITM_MOVING_MIN:
      return "ITM_MOVING_MIN";
    case ITM_MOVING_SDEV:
      return "ITM_MOVING_SDEV";
    case ITM_MOVING_VARIANCE:
      return "ITM_MOVING_VARIANCE";
    case ITM_OFFSET:
      return "ITM_OFFSET";
    case ITM_RUNNING_COUNT:
      return "ITM_RUNNING_COUNT";
    case ITM_ROWS_SINCE:
      return "ITM_ROWS_SINCE";
    case ITM_RUNNING_SUM:
      return "ITM_RUNNING_SUM";
    case ITM_RUNNING_AVG:
      return "ITM_RUNNING_AVG";
    case ITM_RUNNING_MAX:
      return "ITM_RUNNING_MAX";
    case ITM_RUNNING_MIN:
      return "ITM_RUNNING_MIN";
    case ITM_RUNNING_SDEV:
      return "ITM_RUNNING_SDEV";
    case ITM_RUNNING_VARIANCE:
      return "ITM_RUNNING_VARIANCE";
    case ITM_THIS:
      return "ITM_THIS";
    case ITM_NOT_THIS:
      return "ITM_NOT_THIS";
    case ITM_RUNNING_PIVOT_GROUP:
      return "ITM_RUNNING_PIVOT_GROUP";

    // flow control
    case ITM_DO_WHILE:
      return "ITM_DO_WHILE";
    case ITM_BLOCK:
      return "ITM_BLOCK";
    case ITM_WHILE:
      return "ITM_WHILE";

      // scalar min/max

    case ITM_SCALAR_MIN:
      return "ITM_SCALAR_MIN";
    case ITM_SCALAR_MAX:
      return "ITM_SCALAR_MAX";

    case ITM_CURRENT_TIMESTAMP_RUNNING:
      return "ITM_CURRENT_TIMESTAMP_RUNNING";

    // numeric functions
    case ITM_ABS:
      return "ITM_ABS";
    case ITM_CEIL:
      return "ITM_CEIL";
    case ITM_COS:
      return "ITM_COS";
    case ITM_COSH:
      return "ITM_COSH";
    case ITM_FLOOR:
      return "ITM_FLOOR";
    case ITM_LOG:
      return "ITM_LOG";
    case ITM_LOG10:
      return "ITM_LOG10";
    case ITM_LOG2:
      return "ITM_LOG2";
    case ITM_MOD:
      return "ITM_MOD";
    case ITM_POWER:
      return "ITM_POWER";
    case ITM_ROUND:
      return "ITM_ROUND";
    case ITM_SIGN:
      return "ITM_SIGN";
    case ITM_SIN:
      return "ITM_SIN";
    case ITM_SINH:
      return "ITM_SINH";
    case ITM_SQRT:
      return "ITM_SQRT";
    case ITM_TAN:
      return "ITM_TAN";
    case ITM_TANH:
      return "ITM_TANH";
    case ITM_COT:
      return "ITM_COT";
    case ITM_ROUND_ROBIN:
      return "ITM_ROUND_ROBIN";
    case ITM_ACOS:
      return "ITM_ACOS";
    case ITM_ASIN:
      return "ITM_ASIN";
    case ITM_ATAN:
      return "ITM_ATAN";
    case ITM_ATAN2:
      return "ITM_ATAN2";
    case ITM_DEGREES:
      return "ITM_DEGREES";
    case ITM_EXP:
      return "ITM_EXP";
    case ITM_PI:
      return "ITM_PI";
    case ITM_RADIANS:
      return "ITM_RADIANS";
    case ITM_SCALE_TRUNC:
      return "ITM_SCALE_TRUNC";
    case ITM_MASK_CLEAR:
      return "ITM_MASK_CLEAR";
    case ITM_MASK_SET:
      return "ITM_MASK_SET";
    case ITM_SHIFT_RIGHT:
      return "ITM_SHIFT_RIGHT";
    case ITM_SHIFT_LEFT:
      return "ITM_SHIFT_LEFT";
    case ITM_BITAND:
      return "ITM_BITAND";
    case ITM_BITOR:
      return "ITM_BITOR";
    case ITM_BITXOR:
      return "ITM_BITXOR";
    case ITM_BITNOT:
      return "ITM_BITNOT";
    case ITM_BITEXTRACT:
      return "ITM_BITEXTRACT";

    // string functions
    case ITM_TRUNC:
      return "ITM_TRUNC";
    case ITM_ASCII:
      return "ITM_ASCII";
    case ITM_CODE_VALUE:
      return "ITM_CODE_VALUE";
    case ITM_POSITION:
      return "ITM_POSITION";
    case ITM_CHAR_LENGTH:
      return "ITM_CHAR_LENGTH";
    case ITM_INSERT_STR:
      return "ITM_INSERT_STR";
    case ITM_OCTET_LENGTH:
      return "ITM_OCTET_LENGTH";
    case ITM_LOWER:
      return "ITM_LOWER";
    case ITM_LPAD:
      return "ITM_LPAD";
    case ITM_LTRIM:
      return "ITM_LTRIM";
    case ITM_REPLACE:
      return "ITM_REPLACE";
    case ITM_REGEXP_REPLACE:
      return "ITM_REGEXP_REPLACE";
    case ITM_REGEXP_SUBSTR:
      return "ITM_REGEXP_SUBSTR";
    case ITM_REGEXP_COUNT:
      return "ITM_REGEXP_COUNT";
    case ITM_RPAD:
      return "ITM_RPAD";
    case ITM_RTRIM:
      return "ITM_RTRIM";
    case ITM_SOUNDEX:
      return "ITM_SOUNDEX";
    case ITM_SUBSTR:
      return "ITM_SUBSTR";
    case ITM_TRIM:
      return "ITM_TRIM";
    case ITM_UPPER:
      return "ITM_UPPER";
    case ITM_CHAR:
      return "ITM_CHAR";
    case ITM_CONCAT:
      return "ITM_CONCAT";
    case ITM_UNPACKCOL:
      return "ITM_UNPACKCOL";
    case ITM_EXPLODE_VARCHAR:
      return "ITM_EXPLODE_VARCHAR";
    case ITM_REPEAT:
      return "ITM_REPEAT";
    case ITM_RIGHT:
      return "ITM_RIGHT";
    case ITM_CONVERTTOBITS:
      return "ITM_CONVERTTOBITS";
    case ITM_CONVERTTOHEX:
      return "ITM_CONVERTTOHEX";
    case ITM_CONVERTFROMHEX:
      return "ITM_CONVERTFROMHEX";
    case ITM_TOKENSTR:
      return "ITM_TOKENSTR";
    case ITM_REVERSE:
      return "ITM_REVERSE";

    // UNICODE/DOUBLEBYTE charsets built-in functions
    case ITM_SUBSTR_DOUBLEBYTE:
      return "ITM_SUBSTR_DOUBLEBYTE";
    case ITM_TRIM_DOUBLEBYTE:
      return "ITM_TRIM_DOUBLEBYTE";
    case ITM_CHAR_LENGTH_DOUBLEBYTE:
      return "ITM_CHAR_LENGTH_DOUBLEBYTE";
    case ITM_LIKE_DOUBLEBYTE:
      return "ITM_LIKE_DOUBLEBYTE";
    case ITM_UPPER_UNICODE:
      return "ITM_UPPER_UNICODE";
    case ITM_LOWER_UNICODE:
      return "ITM_LOWER_UNICODE";
    case ITM_REPEAT_UNICODE:
      return "ITM_REPEAT_UNICODE";
    case ITM_REPLACE_UNICODE:
      return "ITM_REPLACE_UNICODE";
    case ITM_UNICODE_CODE_VALUE:
      return "ITM_UNICODE_CODE_VALUE";
    case ITM_NCHAR_MP_CODE_VALUE:
      return "ITM_NCHAR_MP_CODE_VALUE";
    // translate function
    case ITM_TRANSLATE:
      return "ITM_TRANSLATE";

    case ITM_UNICODE_CHAR:
      return "ITM_UNICODE_CHAR";
    case ITM_NCHAR_MP_CHAR:
      return "ITM_NCHAR_MP_CHAR";

    // RowSet expression functions
    case ITM_ROWSETARRAY_SCAN:
      return "ITM_ROWSETARRAY_SCAN";
    case ITM_ROWSETARRAY_ROWID:
      return "ITM_ROWSETARRAY_ROWID";
    case ITM_ROWSETARRAY_INTO:
      return "ITM_ROWSETARRAY_INTO";

    case ITM_LEFT:
      return "ITM_LEFT";
    case ITM_SPACE:
      return "ITM_SPACE";
    case ITM_ODBC_LENGTH:
      return "ITM_ODBC_LENGTH";

    // datetime functions
    case ITM_CONVERTTIMESTAMP:
      return "ITM_CONVERTTIMESTAMP";
    case ITM_DATEFORMAT:
      return "ITM_DATEFORMAT";
    case ITM_NUMBERFORMAT:
      return "ITM_NUMBERFORMAT";
    case ITM_DAYOFWEEK:
      return "ITM_DAYOFWEEK";
    case ITM_EXTRACT:
      return "ITM_EXTRACT";
    case ITM_INITCAP:
      return "ITM_INITCAP";
    case ITM_JULIANTIMESTAMP:
      return "ITM_JULIANTIMESTAMP";
    case ITM_EXTRACT_ODBC:
      return "ITM_EXTRACT_ODBC";
    case ITM_DAYNAME:
      return "ITM_DAYNAME";
    case ITM_MONTHNAME:
      return "ITM_MONTHNAME";
    case ITM_QUARTER:
      return "ITM_QUARTER";
    case ITM_WEEK:
      return "ITM_WEEK";
    case ITM_DAYOFYEAR:
      return "ITM_DAYOFYEAR";
    case ITM_FIRSTDAYOFYEAR:
      return "ITM_FIRSTDAYOFYEAR";
    case ITM_INTERNALTIMESTAMP:
      return "ITM_INTERNALTIMESTAMP";
    // misc. functions
    case ITM_NARROW:
      return "ITM_NARROW";
    case ITM_INTERVAL:
      return "ITM_INTERVAL";
    case ITM_INSTANTIATE_NULL:
      return "ITM_INSTANTIATE_NULL";
    case ITM_INCREMENT:
      return "ITM_INCREMENT";
    case ITM_DECREMENT:
      return "ITM_DECREMENT";
    case ITM_GREATER_OR_GE:
      return "ITM_GREATER_OR_GE";
    case ITM_LESS_OR_LE:
      return "ITM_LESS_OR_LE";
    case ITM_RANGE_LOOKUP:
      return "ITM_RANGE_LOOKUP";
    case ITM_DECODE:
      return "ITM_DECODE";
    case ITM_HDPHASHCOMB:
      return "ITM_HDPHASHCOMB";
    case ITM_RANDOMNUM:
      return "ITM_RANDOMNUM";
    case ITM_PROGDISTRIB:
      return "ITM_PROGDISTRIB";
    case ITM_HASHCOMB:
      return "ITM_HASHCOMB";
    case ITM_HDPHASH:
      return "ITM_HDPHASH";
    case ITM_EXEC_COUNT:
      return "ITM_EXEC_COUNT";
    case ITM_CURR_TRANSID:
      return "ITM_CURR_TRANSID";
    case ITM_NOTCOVERED:
      return "ITM_NOTCOVERED";
    case ITM_BALANCE:
      return "ITM_BALANCE";
    case ITM_RAND_SELECTION:
      return "ITM_RAND_SELECTION";
    case ITM_PROGDISTRIBKEY:
      return "ITM_PROGDISTRIBKEY";
    case ITM_PAGROUP:
      return "ITM_PAGROUP";
    case ITM_HASH2_DISTRIB:
      return "ITM_HASH2_DISTRIB";

    case ITM_HEADER:
      return "ITM_HEADER";

    case ITM_LOBLENGTH:
      return "ITM_LOBLENGTH";

    case ITM_UNIQUE_EXECUTE_ID:
      return "ITM_UNIQUE_EXECUTE_ID";
    case ITM_GET_TRIGGERS_STATUS:
      return "ITM_GET_TRIGGERS_STATUS";
    case ITM_GET_BIT_VALUE_AT:
      return "ITM_GET_BIT_VALUE_AT";
    case ITM_CURRENTEPOCH:
      return "ITM_CURRENTEPOCH";
    case ITM_VSBBROWTYPE:
      return "ITM_VSBBROWTYPE";
    case ITM_VSBBROWCOUNT:
      return "ITM_VSBBROWCOUNT";
    case ITM_IS_BITWISE_AND_TRUE:
      return "ITM_IS_BITWISE_AND_TRUE";

    case ITM_NULLIFZERO:
      return "ITM_NULLIFZERO";
    case ITM_NVL:
      return "ITM_NVL";

    // for ngram
    case ITM_FIRSTNGRAM:
      return "ITM_FIRSTNGRAM";
    case ITM_NGRAMCOUNT:
      return "ITM_NGRAMCOUNT";
    // subqueries
    case ITM_ROW_SUBQUERY:
      return "ITM_ROW_SUBQUERY";
    case ITM_IN_SUBQUERY:
      return "ITM_IN_SUBQUERY";
    case ITM_IN:
      return "ITM_IN";
    case ITM_EXISTS:
      return "ITM_EXISTS";
    case ITM_NOT_EXISTS:
      return "ITM_NOT_EXISTS";
    case ITM_EQUAL_ALL:
      return "ITM_EQUAL_ALL";
    case ITM_EQUAL_ANY:
      return "ITM_EQUAL_ANY";
    case ITM_NOT_EQUAL_ALL:
      return "ITM_NOT_EQUAL_ALL";
    case ITM_NOT_EQUAL_ANY:
      return "ITM_NOT_EQUAL_ANY";
    case ITM_LESS_ALL:
      return "ITM_LESS_ALL";
    case ITM_LESS_ANY:
      return "ITM_LESS_ANY";
    case ITM_GREATER_ALL:
      return "ITM_GREATER_ALL";
    case ITM_GREATER_ANY:
      return "ITM_GREATER_ANY";
    case ITM_LESS_EQ_ALL:
      return "ITM_LESS_EQ_ALL";
    case ITM_LESS_EQ_ANY:
      return "ITM_LESS_EQ_ANY";
    case ITM_GREATER_EQ_ALL:
      return "ITM_GREATER_EQ_ALL";
    case ITM_GREATER_EQ_ANY:
      return "ITM_GREATER_EQ_ANY";

    case ITM_WILDCARD_EQ_NE:
      return "ITM_WILDCARD_EQ_NE";

    // renaming, conversion, assignment
    case ITM_RENAME_COL:
      return "ITM_RENAME_COL";
    case ITM_CONVERT:
      return "ITM_CONVERT";
    case ITM_CAST:
      return "ITM_CAST";
    case ITM_ASSIGN:
      return "ITM_ASSIGN";

    // convert an NA-type to an item expression
    case ITM_NATYPE:
      return "ITM_NATYPE";

    // do a cast but adjust target length based
    // on operand (used by ODBC)
    case ITM_CAST_CONVERT:
      return "ITM_CAST_CONVERT";

    case ITM_CAST_TYPE:
      return "ITM_CAST_TYPE";

    // for OperatorType::match() of ItemExpr::origOpType()
    case ITM_ANY_AGGREGATE:
      return "ITM_ANY_AGGREGATE";

    // to match Cast, Cast_Convert, Instantiate_Null, Narrow
    case ITM_ANY_CAST:
      return "ITM_ANY_CAST";

    // item expressions describing constraints
    case ITM_CHECK_CONSTRAINT:
      return "ITM_CHECK_CONSTRAINT";
    case ITM_CARD_CONSTRAINT:
      return "ITM_CARD_CONSTRAINT";
    case ITM_UNIQUE_CONSTRAINT:
      return "ITM_UNIQUE_CONSTRAINT";
    case ITM_REF_CONSTRAINT:
      return "ITM_REF_CONSTRAINT";
    case ITM_UNIQUE_OPT_CONSTRAINT:
      return "ITM_UNIQUE_OPT_CONSTRAINT";
    case ITM_FUNC_DEPEND_CONSTRAINT:
      return "ITM_FUNC_DEPEND_CONSTRAINT";

    // list of item expressions
    case ITM_ITEM_LIST:
      return "ITM_ITEM_LIST";

    // leaf nodes of item expressions
    case ITM_CONSTANT:
      return "ITM_CONSTANT";
    case ITM_REFERENCE:
      return "ITM_REFERENCE";
    case ITM_BASECOLUMN:
      return "ITM_BASECOLUMN";
    case ITM_INDEXCOLUMN:
      return "ITM_INDEXCOLUMN";
    case ITM_HOSTVAR:
      return "ITM_HOSTVAR";
    case ITM_DYN_PARAM:
      return "ITM_DYN_PARAM";
    case ITM_SEL_INDEX:
      return "ITM_SEL_INDEX";
    case ITM_VALUEIDREF:
      return "ITM_VALUEIDREF";
    case ITM_VALUEIDUNION:
      return "ITM_VALUEIDUNION";
    case ITM_VEG:
      return "ITM_VEG";
    case ITM_VEG_PREDICATE:
      return "ITM_VEG_PREDICATE";
    case ITM_VEG_REFERENCE:
      return "ITM_VEG_REFERENCE";
    case ITM_DEFAULT_SPECIFICATION:
      return "ITM_DEFAULT_SPECIFICATION";
    case ITM_SAMPLE_VALUE:
      return "ITM_SAMPLE_VALUE";
    case ITM_CACHE_PARAM:
      return "ITM_CACHE_PARAM";

    // Item expressions for transactions
    case ITM_SET_TRANS_ISOLATION_LEVEL:
      return "ITM_SET_TRANS_ISOLATION_LEVEL";
    case ITM_SET_TRANS_ACCESS_MODE:
      return "ITM_SET_TRANS_ACCESS_MODE";
    case ITM_SET_TRANS_DIAGS:
      return "ITM_SET_TRANS_DIAGS";
    case ITM_SET_TRANS_ROLLBACK_MODE:
      return "ITM_SET_TRANS_ROLLBACK_MODE";
    case ITM_SET_TRANS_AUTOABORT_INTERVAL:
      return "ITM_SET_TRANS_AUTOABORT_INTERVAL";
    case ITM_SET_TRANS_MULTI_COMMIT:
      return "ITM_SET_TRANS_MULTI_COMMIT";

    case ITM_LAST_ITEM_OP:
      return "ITM_LAST_ITEM_OP";
    case ITM_UNIQUE_ID:
      return "ITM_UNIQUE_ID";
    case ITM_UNIQUE_ID_SYS_GUID:
      return "ITM_UNIQUE_ID_SYS_GUID";
    case ITM_UNIQUE_SHORT_ID:
      return "ITM_UNIQUE_SHORT_ID";
    case ITM_ROWNUM:
      return "ITM_ROWNUM";
    case ITM_HBASE_COLUMN_LOOKUP:
      return "ITM_HBASE_COLUMN_LOOKUP";
    case ITM_HBASE_COLUMNS_DISPLAY:
      return "ITM_HBASE_COLUMNS_DISPLAY";
    case ITM_HBASE_COLUMN_CREATE:
      return "ITM_HBASE_COLUMN_CREATE";
    case ITM_HBASE_VISIBILITY:
      return "ITM_HBASE_VISIBILITY";
    case ITM_HBASE_VISIBILITY_SET:
      return "ITM_HBASE_VISIBILITY_SET";
    case ITM_HBASE_TIMESTAMP:
      return "ITM_HBASE_TIMESTAMP";
    case ITM_HBASE_VERSION:
      return "ITM_HBASE_VERSION";
    case ITM_HBASE_ROWID:
      return "ITM_HBASE_ROWID";

    case ITM_SEQUENCE_VALUE:
      return "ITM_SEQUENCE_VALUE";
    case ITM_RANGE_VALUES_MERGE:
      return "ITM_RANGE_VALUES_MERGE";
    case ITM_RANGE_VALUES_INSERT:
      return "ITM_RANGE_VALUES_INSERT";
    case ITM_RANGE_VALUES_IN:
      return "ITM_RANGE_VALUES_IN";
    case ITM_RANGE_VALUES_COPY:
      return "ITM_RANGE_VALUES_COPY";
    case ITM_RANGE_VALUES_UNPACK:
      return "ITM_RANGE_VALUES_UNPACK";

    case ITM_COMPOSITE_CONCAT:
      return "COMPOSITE_CONCAT";
    case ITM_COMPOSITE_CREATE:
      return "COMPOSITE_CREATE";
    case ITM_COMPOSITE_DISPLAY:
      return "COMPOSITE_DISPLAY";
    case ITM_COMPOSITE_CAST:
      return "COMPOSITE_CAST";
    case ITM_COMPOSITE_EXTRACT:
      return "COMPOSITE_EXTRACT";
    case ITM_COMPOSITE_ARRAY_LENGTH:
      return "COMPOSITE_ARRAY_LENGTH";
    case ITM_COMPOSITE_ARRAY_CAST:
      return "COMPOSITE_ARRAY_CAST";
    case ITM_COMPOSITE_HIVE_CAST:
      return "COMPOSITE_HIVE_CAST";

    case ITM_ENCODE_BASE64:
      return "ITM_ENCODE_BASE64";
    case ITM_DECODE_BASE64:
      return "ITM_DECODE_BASE64";
    case ITM_BEGINKEY:
      return "ITM_BEGINKEY";
    case ITM_ENDKEY:
      return "ITM_ENDKEY";

    // Note, this list is arranged in the same order as the types
    // appear in common/OperTypeEnum.h, please keep the same order
    // when adding new types
    default: {
      cout << "OperatorType must be added to getOperTypeEnumAsString()" << ote << endl;
      return "Add To getOperTypeEnumAsString()";
    }
  }
}

char *exClauseGetText(OperatorTypeEnum ote) {
  char *itmText = (char *)getOperTypeEnumAsString(ote);

  // strip the ITM_ prefix
  if ((str_len(itmText) > 4) && (str_cmp(itmText, "ITM_", 4) == 0))
    return &itmText[4];
  else
    return itmText;
}  // exClausegetText()

void ex_clause::displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea) {
  return displayContents(space, displayStr, clauseNum, constsArea, 0, -1, NULL);
}

void ex_clause::displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea, UInt32 flag) {
  return displayContents(space, displayStr, clauseNum, constsArea, 0, -1, NULL);
}

void ex_clause::displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea,
                                UInt32 clauseFlags, Int16 instruction, const char *instrText) {
  char buf[100];
  if (displayStr) {
    if (compExprNum > 0)
      str_sprintf(buf, "  Clause L%d:#%d  %s", compExprNum, clauseNum, displayStr);
    else
      str_sprintf(buf, "  Clause #%d: %s", clauseNum, displayStr);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  str_sprintf(buf, "    OperatorTypeEnum = %s(%d), NumOperands = %d", getOperTypeEnumAsString(operType_), operType_,
              numOperands_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    ex_clause::flags_ = %x ", flags_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (displayStr) {
    str_sprintf(buf, "    %s::flags_ = %x ", displayStr, clauseFlags);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (noPCodeAvailable()) {
    str_sprintf(buf, "    PCODE  = not supported ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  } else {
    str_sprintf(buf, "    PCODE  = supported ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (instruction >= 0) {
    if (instrText)
      str_sprintf(buf, "    instruction: %s(%d), instrArrayIndex_: %d", instrText, instruction, instrArrayIndex_);
    else
      str_sprintf(buf, "    instruction: UNKNOWN(%d), instrArrayIndex_: %d", instruction, instrArrayIndex_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (numOperands_ == 0) return;

  if (numOperands_ > 0) {
    NABoolean showplan = getOperand(0)->showplan();

    for (int i = 0; i < numOperands_; i++) {
      getOperand(i)->displayContents(space, i, constsArea, (showplan ? getOperand(i + numOperands_) : NULL));
      str_sprintf(buf, "\n");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }
}

/////////////////////////////////////////////////////////

// Derived clauses
/////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// class ex_arith_clause
///////////////////////////////////////////////////////////
ex_arith_clause::ex_arith_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space, short arithRoundingMode,
                                 NABoolean divToDownscale)
    : ex_clause(ex_clause::ARITH_TYPE, oper_type, (oper_type == ITM_NEGATE ? 2 : 3), attr, space), flags_(0) {
  setAugmentedAssignOperation(TRUE);
  arithRoundingMode_ = (char)arithRoundingMode;

  if (divToDownscale) setDivToDownscale(TRUE);

  if (attr) setInstruction();
}

ExRegexpClauseChar::ExRegexpClauseChar(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space)
    : ExRegexpClauseBase(oper_type, num_operands, attr, space) {}

ex_arith_clause::ex_arith_clause(clause_type type, OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_clause(type, oper_type, 3, attr, space),
      arithRoundingMode_(0),
      flags_(0)

{
  setAugmentedAssignOperation(TRUE);
  setInstruction();
}

ex_arith_sum_clause::ex_arith_sum_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_arith_clause(ex_clause::ARITH_SUM_TYPE, oper_type, attr, space) {}

ex_arith_count_clause::ex_arith_count_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_arith_clause(ex_clause::ARITH_COUNT_TYPE, oper_type, attr, space) {}

///////////////////////////////////////////////////////////
// class ex_comp_clause
///////////////////////////////////////////////////////////
ex_comp_clause::ex_comp_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space, int flags)
    : ex_clause(ex_clause::COMP_TYPE, oper_type, 3, attr, space), flags_(0), rollupColumnNum_(-1) {
  if (flags) setSpecialNulls();
  setInstruction();
}

///////////////////////////////////////////////////////////
// class ex_conv_clause
///////////////////////////////////////////////////////////
ex_conv_clause::ex_conv_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space, short num_operands,
                               NABoolean checkTruncErr, NABoolean reverseDataErrorConversionFlag,
                               NABoolean noStringTruncWarnings, NABoolean convertToNullWhenErrorFlag,
                               NABoolean padUseZero)
    : ex_clause(ex_clause::CONV_TYPE, oper_type, num_operands, attr, space),
      lastVOAoffset_(0),
      lastVcIndicatorLength_(0),
      lastNullIndicatorLength_(0),
      computedLength_(0),
      alignment_(0),
      flags_(0) {
  if (oper_type == ITM_NARROW)
    // Narrow reports conversion errors via a variable instead of a
    // SQL diagnostic -- so in this case we want to handle NULLs ourselves
    setProcessNulls();
  if (checkTruncErr) setCheckTruncationFlag();

  if (reverseDataErrorConversionFlag) flags_ |= REVERSE_DATA_ERROR_CONVERSION_FLAG;

  if (noStringTruncWarnings) setNoTruncationWarningsFlag();

  if (convertToNullWhenErrorFlag) flags_ |= CONV_TO_NULL_WHEN_ERROR;

  if (padUseZero) flags_ |= PAD_USE_ZERO;

  setInstruction();
}

///////////////////////////////////////////////////////////
// class ex_inout_clause
///////////////////////////////////////////////////////////
ex_inout_clause::ex_inout_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_clause(ex_clause::INOUT_TYPE, oper_type, 1, attr, space) {
  name = 0;
  heading_ = 0;
  //  convHVClause_ = 0;
  flags_ = 0;
}

///////////////////////////////////////////////////////////
// class bool_result_clause
///////////////////////////////////////////////////////////
bool_result_clause::bool_result_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_clause(ex_clause::BOOL_RESULT_TYPE, oper_type, 1, attr, space) {}

///////////////////////////////////////////////////////////
// class ex_branch_clause
///////////////////////////////////////////////////////////
ex_branch_clause::ex_branch_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_clause(ex_clause::BRANCH_TYPE, oper_type, 2, attr, space), saved_next_clause(NULL), branch_clause(NULL) {}

ex_branch_clause::ex_branch_clause(OperatorTypeEnum oper_type, Space *space)
    : ex_clause(ex_clause::BRANCH_TYPE, oper_type, 0, NULL, space), saved_next_clause(NULL), branch_clause(NULL) {}

///////////////////////////////////////////////////////////
// class ex_bool_clause
///////////////////////////////////////////////////////////
ex_bool_clause::ex_bool_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_clause(ex_clause::BOOL_TYPE, oper_type, 3, attr, space) {}

///////////////////////////////////////////////////////////
// class ex_unlogic_clause
///////////////////////////////////////////////////////////
ex_unlogic_clause::ex_unlogic_clause(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
    : ex_clause(ex_clause::UN_LOGIC_TYPE, oper_type, 2, attr, space) {}

///////////////////////////////////////////////////////////
// class ex_aggregate_clause
///////////////////////////////////////////////////////////
ex_aggregate_clause::ex_aggregate_clause(OperatorTypeEnum oper_type, short num_operands, Attributes **attr,
                                         Space *space)
    : ex_clause(ex_clause::AGGREGATE_TYPE, oper_type, num_operands, attr, space) {}

///////////////////////////////////////////////////////////
// class ex_noop_clause
///////////////////////////////////////////////////////////
ex_noop_clause::ex_noop_clause() : ex_clause(ex_clause::NOOP_TYPE, ITM_CONVERT, 0, 0, 0) {}

/////////////////////////////////////////////////////////
// class ex_function_clause
/////////////////////////////////////////////////////////
ex_function_clause::ex_function_clause(OperatorTypeEnum oper_type, short num_operands, Attributes **attr, Space *space)
    : ex_clause(ex_clause::FUNCTION_TYPE, oper_type, num_operands, attr, space), origFunctionOperType_(oper_type) {
  setDerivedFunction(FALSE);

  userTextStr_[0] = 0;
}

/////////////////////////////////////////////////////////
// class ex_like_clause_char
/////////////////////////////////////////////////////////
ex_like_clause_char::ex_like_clause_char(OperatorTypeEnum oper_type, short num_operands, NABoolean allowEscapeNull,
                                         Attributes **attr, Space *space)
    : ex_like_clause_base(oper_type, num_operands, attr, space) {
  setAllowEscapeNull(allowEscapeNull);
}

ex_like_clause_doublebyte::ex_like_clause_doublebyte(OperatorTypeEnum oper_type, short num_operands,
                                                     NABoolean allowEscapeNull, Attributes **attr, Space *space)
    : ex_like_clause_base(oper_type, num_operands, attr, space) {
  setAllowEscapeNull(allowEscapeNull);
}

/////////////////////////////////////////////////////////////
// Methods to display Contents
/////////////////////////////////////////////////////////////
void ex_aggr_one_row_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                             char *constsArea) {
  ex_clause::displayContents(space, "ex_aggr_one_row_clause", clauseNum, constsArea);
}

void ex_aggr_any_true_max_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                                  char *constsArea) {
  ex_clause::displayContents(space, "ex_aggr_any_true_max_clause", clauseNum, constsArea);
}
void ex_aggr_min_max_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                             char *constsArea) {
  ex_clause::displayContents(space, "ex_aggr_min_max_clause", clauseNum, constsArea);
}

void ex_pivot_group_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                            char *constsArea) {
  ex_clause::displayContents(space, "ex_pivot_group_clause", clauseNum, constsArea);
}

void ExFunctionGrouping::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ExFunctionGrouping", clauseNum, constsArea);

  char buf[100];
  str_sprintf(buf, "    rollupGroupIndex_ = %d\n", rollupGroupIndex_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
}

void ex_arith_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  setInstruction();

  char buf[100];
  str_sprintf(buf, "  Clause #%d: ex_arith_clause", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (arithRoundingMode_ != 0) {
    str_sprintf(buf, "    arithRoundingMode_ = %d, divToScale = %d", (short)arithRoundingMode_,
                (getDivToDownscale() ? 1 : 0));
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea, 0,
                             ex_arith_clause::getInstruction(getInstrArrayIndex()),
                             ex_arith_clause::getInstructionStr(getInstrArrayIndex()));
}

void ex_arith_sum_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  setInstruction();
  ex_clause::displayContents(space, "ex_arith_sum_clause", clauseNum, constsArea);
}

void ex_arith_count_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                            char *constsArea) {
  setInstruction();
  ex_clause::displayContents(space, "ex_arith_count_clause", clauseNum, constsArea);
}

void ex_bool_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_bool_clause", clauseNum, constsArea);
}

void bool_result_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "bool_result_clause", clauseNum, constsArea);
}

void ex_branch_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ex_branch_clause", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    OperatorTypeEnum = %s(%d), NumOperands = %d", getOperTypeEnumAsString(getOperType()),
              getOperType(), getNumOperands());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    flags_ = %x ", getAllFlags());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (noPCodeAvailable()) {
    str_sprintf(buf, "    PCODE  = not supported ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  } else {
    str_sprintf(buf, "    PCODE  = supported ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }
  str_sprintf(buf, "    branch to = #%d ", branch_clause->clauseNum());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (getNumOperands() == 0) return;

  if (getNumOperands() > 0) {
    NABoolean showplan = getOperand(0)->showplan();

    for (int i = 0; i < getNumOperands(); i++) {
      getOperand(i)->displayContents(space, i, constsArea, (showplan ? getOperand(i + getNumOperands()) : NULL));
      str_sprintf(buf, "\n");
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }
}

void ex_comp_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  setInstruction();

  char buf[100];
  str_sprintf(buf, "  Clause #%d: ex_comp_clause", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    ex_comp_clause::rollupColumnNum_ = %d", rollupColumnNum_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    ex_comp_clause::flags_ = %x", flags_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea, 0,
                             ex_comp_clause::getInstruction(getInstrArrayIndex()),
                             ex_comp_clause::getInstructionStr(getInstrArrayIndex()));
}

void ex_conv_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  setInstruction();
  ex_clause::displayContents(space, "ex_conv_clause", clauseNum, constsArea, flags_,
                             ex_conv_clause::getInstruction(getInstrArrayIndex()),
                             ex_conv_clause::getInstructionStr(getInstrArrayIndex()));
}

void ex_function_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_function_clause", clauseNum, constsArea);
}
void ex_function_abs::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_function_abs", clauseNum, constsArea);
}

void ExFunctionMath::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ExFunctionMath", clauseNum, constsArea);
}

void ExFunctionBitOper::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ExFunctionBitOper", clauseNum, constsArea);
}
void ex_inout_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_inout_clause", clauseNum, constsArea);
  //  cout << "Name  = " << getName() << endl;
}

void ex_noop_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_noop_clause", clauseNum, constsArea);
}

void ex_unlogic_clause::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_unlogic_clause", clauseNum, constsArea);
}

void ex_like_clause_char::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_like_clause_char", clauseNum, constsArea);
}

void ExRegexpClauseChar::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ExRegexpClauseChar", clauseNum, constsArea);
}

void ex_like_clause_doublebyte::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                                char *constsArea) {
  ex_clause::displayContents(space, "ex_like_clause_doublebyte", clauseNum, constsArea);
}

void ExFunctionHbaseVisibility::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                                char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ExFunctionHbaseVisibility", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    colIndex_ = %d", colIndex_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea);
}

void ExFunctionHbaseVisibilitySet::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                                   char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ExFunctionHbaseVisibilitySet", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    colID_ = %s", colID_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea);
}

void ExFunctionHbaseTimestamp::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                               char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ExFunctionHbaseTimestamp", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    colIndex_ = %d", colIndex_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea);
}

void ExFunctionHbaseVersion::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                             char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ExFunctionHbaseVersion", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    colIndex_ = %d", colIndex_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea);
}

void ExFunctionHbaseRowid::displayContents(Space *space, const char * /*displayStr*/, int clauseNum, char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ExFunctionHbaseRowid", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea);
}

void ex_function_dateformat::displayContents(Space *space, const char * /*displayStr*/, int clauseNum,
                                             char *constsArea) {
  char buf[100];
  str_sprintf(buf, "  Clause #%d: ex_function_dateformat", clauseNum);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    dateformat_ = %d", dateformat_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, (const char *)NULL, clauseNum, constsArea);
}

// Function to compare two strings.
int charStringCompareWithPad(char *in_s1, int length1, char *in_s2, int length2, char padChar, NABoolean ignoreSpace) {
  static char *isTrailingBlankSensitive = NULL;
  bool strictCmp = false;
  unsigned char *s1 = (unsigned char *)in_s1;
  unsigned char *s2 = (unsigned char *)in_s2;

  int compare_len;
  int compare_code;

  if (isTrailingBlankSensitive == NULL) {
    isTrailingBlankSensitive = getenv("VARCHAR_TRAILING_BLANK_SENSITIVE");
  }
  if (isTrailingBlankSensitive && atoi(isTrailingBlankSensitive) == 1 && !ignoreSpace) strictCmp = true;

  if (length1 > length2)
    compare_len = length2;
  else
    compare_len = length1;

  compare_code = str_cmp(in_s1, in_s2, compare_len);

  if ((compare_code == 0) && (length1 != length2)) {
    if (length1 > length2) {
      int j = compare_len;

      while ((j < length1) && (compare_code == 0)) {
        if (s1[j] < padChar)
          compare_code = -1;
        else if (s1[j] > padChar)
          compare_code = 1;
        else if (strictCmp)
          compare_code = 1;
        j++;
      }
    } else {
      int j = compare_len;

      while ((j < length2) && (compare_code == 0)) {
        if (s2[j] < padChar)
          compare_code = 1;
        else if (s2[j] > padChar)
          compare_code = -1;
        else if (strictCmp)
          compare_code = -1;
        j++;
      }
    }
  }

  // return 0,1,-1 values, not the positive, 0, negative
  if (compare_code > 0) compare_code = 1;
  if (compare_code < 0) compare_code = -1;
  return compare_code;
}

int wcharStringCompareWithPad(NAWchar *s1, int length1, NAWchar *s2, int length2, NAWchar space) {
  char *isTrailingBlankSensitive = getenv("VARCHAR_TRAILING_BLANK_SENSITIVE");
  bool strictCmp = false;
  if (isTrailingBlankSensitive && atoi(isTrailingBlankSensitive) == 1) strictCmp = true;

  int compare_len;
  int compare_code;

  if (length1 > length2)
    compare_len = length2;
  else
    compare_len = length1;

  compare_code = wc_str_cmp(s1, s2, compare_len);

  if ((compare_code == 0) && (length1 != length2)) {
    if (length1 > length2) {
      int j = compare_len;

      while ((j < length1) && (compare_code == 0)) {
        if (s1[j] < space)
          compare_code = -1;
        else if (s1[j] > space)
          compare_code = 1;
        else if (strictCmp)
          compare_code = 1;
        j++;
      }
    } else {
      int j = compare_len;

      while ((j < length2) && (compare_code == 0)) {
        if (s2[j] < space)
          compare_code = 1;
        else if (s2[j] > space)
          compare_code = -1;
        else if (strictCmp)
          compare_code = -1;
        j++;
      }
    }
  }

  // return 0,1,-1 values, not the positive, 0, negative
  if (compare_code > 0) compare_code = 1;
  if (compare_code < 0) compare_code = -1;
  return compare_code;
}

void ExFunctionRangeOfValues::displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea) {
  ex_clause::displayContents(space, "ex_rangeofvalues_clause", clauseNum, constsArea);
}

ExFunctionRangeOfValues::ExFunctionRangeOfValues()
    : rangeOfValues_(NULL),
      totalSizeCap_(0),
      filterId_(0),
      dumpDebugData_(FALSE),
      treatDateAsTimestamp_(TRUE),
      nativeByteOrder_(FALSE),
      maxNumEntries_(0) {}

ExFunctionRangeOfValues::ExFunctionRangeOfValues(OperatorTypeEnum oper_type, Attributes **attr, Space *space,
                                                 UInt32 cap, UInt32 numChildren, Int16 filterId,
                                                 NABoolean dumpDebugData, NABoolean dateAsTimestamp,
                                                 NABoolean nativeByteOrder, int maxEntries)
    : ex_aggregate_clause(oper_type, numChildren + 1, attr, space),
      rangeOfValues_(NULL),
      totalSizeCap_(cap),
      filterId_(filterId),
      dumpDebugData_(dumpDebugData),
      treatDateAsTimestamp_(dateAsTimestamp),
      nativeByteOrder_(nativeByteOrder),
      maxNumEntries_(maxEntries) {}

ExFunctionRangeOfValues::~ExFunctionRangeOfValues() {
  delete rangeOfValues_;
  rangeOfValues_ = NULL;
}

// defined in exp_conv.cpp
ex_expr::exp_return_type convDecToInt64(long &target, char *source, int sourceLen, CollHeap *heap,
                                        ComDiagsArea **diagsArea, int flags);

Int16 convertDateTimeTimestampToAscii(char *target, int targetLen, char *source, int sourceLen, Int16 precision,
                                      Int16 scale, int format, CollHeap *heap) {
  ExpDatetime srcDatetimeOpType;

  // Setup attribute for the source.
  srcDatetimeOpType.setPrecision(precision);
  srcDatetimeOpType.setScale(scale);

  // Convert the datetime value to ASCII in the DEFAULT format.
  int realTargetLen = srcDatetimeOpType.convDatetimeToASCII(source,     // source
                                                            target,     // target
                                                            targetLen,  // target len
                                                            format, NULL, heap, NULL);

  if (realTargetLen >= 0) {
    assert(realTargetLen < targetLen);
    target[realTargetLen] = NULL;
  }

#if 0
   fstream& out = getPrintHandle();
   out << "ts=" << target ;
   out << ", len=" << realTargetLen << endl;
   out.close();
#endif

  return realTargetLen;
}

void ExFunctionRangeOfValues::displayOpData(const char *msg, char *op_data[], CollHeap *heap) {
  fstream &out = getPrintHandle();
  displayOpData(out, msg, op_data, heap);
  out.close();
}

void ExFunctionRangeOfValues::displayOpData(fstream &out, const char *msg, char *op_data[], CollHeap *heap) {
  out << "RV" << getFilterId() << ": " << msg << "(";

  Attributes *sourceAttr = getOperand(1);
  Int16 dt = sourceAttr->getDatatype();

  char *source = (char *)op_data[1];

  switch (dt) {
    case REC_BIN8_SIGNED:
      out << (*(Int8 *)source);
      break;

    case REC_BIN16_SIGNED:
      out << (*(Int16 *)source);
      break;

    case REC_BIN32_SIGNED:
      out << (*(int *)source);
      break;

    case REC_BIN64_SIGNED:
      out << (*(long *)source);
      break;

    case REC_BIN8_UNSIGNED:
      out << (*(UInt8 *)source);
      break;

    case REC_BIN16_UNSIGNED:
      out << (*(UInt16 *)source);
      break;

    case REC_BIN32_UNSIGNED:
      out << (*(UInt32 *)source);
      break;

    default:
      break;
  }

  if (DFS2REC::isAnyCharacter(dt)) {
    int len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

    if (DFS2REC::isDoubleCharacter(dt)) {
      for (int i = 0; i < len / 2; i++) {
        out << ((wchar_t *)source)[i];
      }
    } else {
      for (int i = 0; i < len; i++) {
        out << source[i];
      }
    }
  }

  if (DFS2REC::isDateTime(dt)) {
    char target[40];

    switch (sourceAttr->getPrecision()) {
      case REC_DATETIME_CODE::REC_DTCODE_DATE: {
        int realTargetLen =
            convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(), REC_DTCODE_DATE,
                                            sourceAttr->getScale(), ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (treatDateAsTimestamp_) {
          strcpy(target + realTargetLen, ZERO_LENGTH_TIMESTAMP);
          realTargetLen += strlen(ZERO_LENGTH_TIMESTAMP);
        }

        for (int i = 0; i < realTargetLen; i++) {
          out << target[i];
        }

        break;
      }

      case REC_DATETIME_CODE::REC_DTCODE_TIME: {
        int realTargetLen =
            convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(), REC_DTCODE_TIME,
                                            sourceAttr->getScale(), ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        for (int i = 0; i < realTargetLen; i++) {
          out << target[i];
        }

        break;
      }

      case REC_DATETIME_CODE::REC_DTCODE_TIMESTAMP: {
        int realTargetLen = convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(),
                                                            REC_DTCODE_TIMESTAMP, sourceAttr->getScale(),
                                                            ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        for (int i = 0; i < realTargetLen; i++) {
          out << target[i];
        }

        break;
      }
    }
  }

  if (DFS2REC::isDecimal(dt)) {
    if (sourceAttr->getScale() == 0) {
      // represent a decimal with 0 scale as an int64
      long value = 0;
      if (convDecToInt64(value, source, sourceAttr->getLength(), heap,
                         NULL,  // ComDiagsArea** diagsArea,
                         0      // flags, ignored due to NULL diagsArea ptr
                         ) != ex_expr::EXPR_OK)
        return;

      // convert to long
      out << value;
    }
  }

  out << ")" << endl;
}

// If the data type can be handled, insert the data and
// return TRUE.  Otherwise, return FALSE.
NABoolean ExFunctionRangeOfValues::insertData(char *op_data[], CollHeap *heap) {
  if (dumpDebugData_) displayOpData("insert", op_data, heap);

  Attributes *sourceAttr = getOperand(1);
  Int16 dt = sourceAttr->getDatatype();

  char *source = (char *)op_data[1];

  switch (dt) {
    case REC_BIN8_SIGNED:
      return rangeOfValues_->insert(*(Int8 *)source);

    case REC_BIN16_SIGNED:
      return rangeOfValues_->insert(*(Int16 *)source);

    case REC_BIN32_SIGNED:
      return rangeOfValues_->insert(*(int *)source);

    case REC_BIN64_SIGNED:
      return rangeOfValues_->insert(*(long *)source);

    case REC_BIN8_UNSIGNED:
      return rangeOfValues_->insert(*(UInt8 *)source);

    case REC_BIN16_UNSIGNED:
      return rangeOfValues_->insert(*(UInt16 *)source);

    case REC_BIN32_UNSIGNED:
      return rangeOfValues_->insert(*(UInt32 *)source);

    default:
      break;
  }

  if (DFS2REC::isAnyCharacter(dt)) {
    int len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

    if (DFS2REC::isDoubleCharacter(dt))
      return rangeOfValues_->insert((wchar_t *)source, len / 2);
    else
      return rangeOfValues_->insert(source, len);
  }

  if (DFS2REC::isDateTime(dt)) {
    char target[40];

    switch (sourceAttr->getPrecision()) {
      case REC_DATETIME_CODE::REC_DTCODE_DATE: {
        int realTargetLen =
            convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(), REC_DTCODE_DATE,
                                            sourceAttr->getScale(), ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (realTargetLen < 0) return FALSE;

        // Address Mantis 7636
        if (treatDateAsTimestamp_) {
          strcpy(target + realTargetLen, ZERO_LENGTH_TIMESTAMP);
          realTargetLen += strlen(ZERO_LENGTH_TIMESTAMP);
        }

        return rangeOfValues_->insertDate(target, realTargetLen);
      }

      case REC_DATETIME_CODE::REC_DTCODE_TIME: {
        int realTargetLen =
            convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(), REC_DTCODE_TIME,
                                            sourceAttr->getScale(), ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (realTargetLen < 0) return FALSE;

        return rangeOfValues_->insertTime(target, realTargetLen);
      }

      case REC_DATETIME_CODE::REC_DTCODE_TIMESTAMP: {
        int realTargetLen = convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(),
                                                            REC_DTCODE_TIMESTAMP, sourceAttr->getScale(),
                                                            ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (realTargetLen < 0) return FALSE;

        return rangeOfValues_->insertTimestamp(target, realTargetLen);
      }
    }
  }

  if (DFS2REC::isDecimal(dt)) {
    if (sourceAttr->getScale() == 0) {
      // represent a decimal with 0 scale as an int64
      long value = 0;
      if (convDecToInt64(value, source, sourceAttr->getLength(), heap,
                         NULL,  // ComDiagsArea** diagsArea,
                         0      // flags, ignored due to NULL diagsArea ptr
                         ) != ex_expr::EXPR_OK)
        return FALSE;

      // convert to long
      rangeOfValues_->insert(value);
      return TRUE;
    }
  }

  return FALSE;
}

NABoolean ExFunctionRangeOfValues::lookupData(char *op_data[], CollHeap *heap) {
  if (dumpDebugData_) displayOpData("lookupInExp", op_data, heap);

  Attributes *sourceAttr = getOperand(1);
  Int16 dt = sourceAttr->getDatatype();

  char *source = (char *)op_data[1];

  switch (dt) {
    case REC_BIN8_SIGNED:
      return rangeOfValues_->lookup(*(Int8 *)source);

    case REC_BIN16_SIGNED:
      return rangeOfValues_->lookup(*(Int16 *)source);

    case REC_BIN32_SIGNED:
      return rangeOfValues_->lookup(*(int *)source);

    case REC_BIN64_SIGNED:
      return rangeOfValues_->lookup(*(long *)source);

    case REC_BIN8_UNSIGNED:
      return rangeOfValues_->lookup(*(UInt8 *)source);

    case REC_BIN16_UNSIGNED:
      return rangeOfValues_->lookup(*(UInt16 *)source);

    case REC_BIN32_UNSIGNED:
      return rangeOfValues_->lookup(*(UInt32 *)source);

    default:
      break;
  }

  if (DFS2REC::isAnyCharacter(dt)) {
    int len = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

    if (DFS2REC::isDoubleCharacter(dt))
      return rangeOfValues_->lookup((wchar_t *)source, len / 2);
    else
      return rangeOfValues_->lookup(source, len);
  }

  if (DFS2REC::isDateTime(dt)) {
    char target[40];

    switch (sourceAttr->getPrecision()) {
      case REC_DATETIME_CODE::REC_DTCODE_DATE: {
        int realTargetLen =
            convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(), REC_DTCODE_DATE,
                                            sourceAttr->getScale(), ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (realTargetLen < 0) return FALSE;

        // Address Mantis 7318: hive/TEST056 fails during jenkins daily run
        //
        // The root cause of the failure is due to the date values stored
        // on disk for ORC and the special treatment to the a date column
        // externalized defined for PARQUET.

        // For ORC, the date type is natively defined.

        // For Parquet, the date type is not allowed and instead a timestamp
        // type can be used as a substitute (with some storage overhead
        // for the TIME values) for the storage format. One can define
        // an external table to map the time-stamp column in Hive
        // to date column in Esgyn.

        // When populating a bloom filter for any DATE column in hash join,
        // a zero-length timestamp (00:00:00.000000) is always appended,
        // to compensate for the lacking of the direct DATE support in PARQUET.

        // However, the lookup code path as needed for the RANGE_IN expression
        // for ORC does not append the same zero-length timestamp.
        // This rejects all the probes to the bloom filters.

        // The fix is to append the same zero-length timestamp
        // for the lookup code path.
        //
        // Also note that the following for a bloom filter:
        //
        //  ORC:     the filter is evaluated in the select predicate only, where
        //           the source date value is of no timestamp part;
        //  PARQUET: the filter is evaluated by the Parquet Reader only, where
        //           the source date value is of timestamp part.

        if (treatDateAsTimestamp_) {
          strcpy(target + realTargetLen, ZERO_LENGTH_TIMESTAMP);
          realTargetLen += strlen(ZERO_LENGTH_TIMESTAMP);
        }

        return rangeOfValues_->lookupDate(target, realTargetLen);
      }

      case REC_DATETIME_CODE::REC_DTCODE_TIME: {
        int realTargetLen =
            convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(), REC_DTCODE_TIME,
                                            sourceAttr->getScale(), ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (realTargetLen < 0) return FALSE;

        return rangeOfValues_->lookupTime(target, realTargetLen);
      }

      case REC_DATETIME_CODE::REC_DTCODE_TIMESTAMP: {
        int realTargetLen = convertDateTimeTimestampToAscii(target, sizeof(target), source, sourceAttr->getLength(),
                                                            REC_DTCODE_TIMESTAMP, sourceAttr->getScale(),
                                                            ExpDatetime::DATETIME_FORMAT_DEFAULT, heap);

        if (realTargetLen < 0) return FALSE;

        return rangeOfValues_->lookupTimestamp(target, realTargetLen);
      }
    }
  }

  if (DFS2REC::isDecimal(dt)) {
    if (sourceAttr->getScale() == 0) {
      // represent a decimal with 0 scale as an int64
      long value = 0;
      if (convDecToInt64(value, source, sourceAttr->getLength(), heap,
                         NULL,  // ComDiagsArea** diagsArea,
                         0      // flags, ignored due to NULL diagsArea ptr
                         ) != ex_expr::EXPR_OK)
        return FALSE;

      // convert to long
      return rangeOfValues_->lookup(value);
    }
  }

  return FALSE;
}

ex_expr::exp_return_type ExFunctionRangeOfValues::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *source = (char *)op_data[1];

  switch (getOperType()) {
    case OperatorTypeEnum::ITM_BITMAP_OR: {
      ClusteredBitmapForIntegers sourceBitMap(totalSizeCap_, heap);

      UInt32 unpackedLen = sourceBitMap.unpack((char *)source);

      // cout << "bit map OR: bits set in source"
      // << sourceBitMap.countOfBitsSet() << endl;

      // if the source bit map is OK (e.g., the space constraint
      // is not violated), perform the OR operation (as operator +=()).
      if (sourceBitMap.isEnabled())
        (*rangeOfValues_) += sourceBitMap;
      else {
        rangeOfValues_->clear();
        rangeOfValues_->setEnable(FALSE);  // disable the target bitMap

        return ex_expr::EXPR_OK;
      }

      break;
    }
    case OperatorTypeEnum::ITM_BITMAP_INSERT: {
      Attributes *sourceAttr = getOperand(1);

      Int16 dt = sourceAttr->getDatatype();

      insertData(op_data, heap);

      break;
    }

    case OperatorTypeEnum::ITM_BITMAP_COPY: {
      UInt32 sourceLen = getOperand(1)->getLength();
      UInt32 targetLen = getOperand(0)->getLength();

      if (sourceLen != targetLen) {
        ExRaiseFunctionSqlError(heap, diagsArea, EXE_RANGE_VALUES_COPY_LENGTHS_DIFF);
        return ex_expr::EXPR_ERROR;
      }

      // set the length (VC length) for the data to be copied
      char *vcLenPtr = op_data[-MAX_OPERANDS];

      Attributes *tgt = getOperand(0);
      int vcLenSize = tgt->getVCIndicatorLength();

      setVCLength(vcLenPtr, vcLenSize, sourceLen);

      unsigned char *target = (unsigned char *)op_data[0];

      // copy the data
      memcpy(target, source, sourceLen);

      return ex_expr::EXPR_OK;

      break;
    }

    case OperatorTypeEnum::ITM_RANGE_VALUES_MERGE: {
      if (!(rangeOfValues_->isEnabled())) return ex_expr::EXPR_OK;

      RangeOfValues *sourceRV = rangeOfValues_->clone(totalSizeCap_, heap);

      UInt32 unpackedLen = sourceRV->unpack((char *)source);

      // if the source is OK (e.g., the space constraint
      // is not violated), perform the MERGE operation (as operator +=()).
      //
      NABoolean isEnabled = sourceRV->isEnabled();

      if (isEnabled) {
        (*rangeOfValues_) += (*sourceRV);

        // check if the resultant filter is useful (i.e., with a
        // low false probability and not many of distinct keys)
        isEnabled = rangeOfValues_->isUseful(maxNumEntries_);
      }

      delete sourceRV;

      if (!isEnabled) {
        rangeOfValues_->clear();
        rangeOfValues_->setEnable(FALSE);  // disable the target bitMap

        return ex_expr::EXPR_OK;
      }

      break;
    }
    case OperatorTypeEnum::ITM_RANGE_VALUES_INSERT: {
      if (!(rangeOfValues_->isEnabled())) return ex_expr::EXPR_OK;

      insertData(op_data, heap);

      break;
    }

    case OperatorTypeEnum::ITM_RANGE_VALUES_COPY: {
      if (!(rangeOfValues_->isEnabled())) return ex_expr::EXPR_OK;

      UInt32 sourceLen = getOperand(1)->getLength();
      UInt32 targetLen = getOperand(0)->getLength();

      if (sourceLen != targetLen) {
        ExRaiseFunctionSqlError(heap, diagsArea, EXE_RANGE_VALUES_COPY_LENGTHS_DIFF);
        return ex_expr::EXPR_ERROR;
      }

      // set the VC length.
      char *vcLenPtr = op_data[-MAX_OPERANDS];

      Attributes *tgt = getOperand(0);
      int vcLenSize = tgt->getVCIndicatorLength();

      setVCLength(vcLenPtr, vcLenSize, sourceLen);

      char *target = (char *)op_data[0];

#if 0
       fstream& out = getPrintHandle();
       Attributes* src = getOperand(1);
       out << "RV copy, source: ";
       out << " data=" << static_cast<void*>(source);
       out << ", int[0]=" << *(int*)source;
       out << ", int[1]=" << *(int*)(source+4);
       out << ", len=" << sourceLen;
       out << ", offset=" << src->getOffset();
       out << "; target:  ";
       out << " data=" << static_cast<void*>(target);
       out << ", offset=" << tgt->getOffset();
       out << ", nullFlag=" << tgt->getNullFlag();
       out << endl;
       out.close();
#endif

      memcpy(target, source, sourceLen);

      return ex_expr::EXPR_OK;

      break;
    }

    case OperatorTypeEnum::ITM_RANGE_VALUES_IN: {
      // init the range spec object rangeOfVaues_
      // with data from operand 2.
      //
      // source is the partition number to test in RS
      // source1 is the range spec.
      if (rangeOfValues_->isEnabled() && rangeOfValues_->entries() == 0) {
        char *rsData = (char *)op_data[2];
        UInt32 rsDataLen = getOperand(2)->getLength();

        UInt32 unpackedLen = rangeOfValues_->unpack(rsData);

#if 0
          rangeOfValues_->dump(cout, "RS operator IN: data unpacked");
#endif
      }

      if (rangeOfValues_->isEnabled())
        // boolean values: 0 = false, 1 = true, -1 = null
        // Only 0 or 1 can be returned from lookupData() call.
        *(int *)op_data[0] = lookupData(op_data, heap);
      else
        *(int *)op_data[0] = 1;

      return ex_expr::EXPR_OK;

      break;
    }

    default:
      ExRaiseFunctionSqlError(heap, diagsArea, EXE_RANGE_VALUES_INVALID_OPERATOR_TYPE);
      return ex_expr::EXPR_ERROR;
  }

#if 0
   rangeOfValues_->dump(cout, "MERGE: the content of the merged RS");
   cout << "merged RS: totalSize=" << rangeOfValues_->getPackedLength();
   cout << ", cap=" << rangeOfValues_->totalSizeCap();
   cout << endl;
#endif

  return ex_expr::EXPR_OK;
}

void ExFunctionRangeOfValues::packRanges(char *target, char *vcLenPtr, int vcLenSize, NABoolean clearWhenDone,
                                         ScanFilterStatsList *sfStats, ScanFilterStats::MergeSemantics semantics) {
  if (rangeOfValues_->isEnabled()) {
    UInt32 packLen = rangeOfValues_->getPackedLength();

    if (rangeOfValues_->totalSizeCap() < packLen || !(rangeOfValues_->isUseful(maxNumEntries_))) {
      rangeOfValues_->setEnable(FALSE);
      rangeOfValues_->clear();
    }
  }

  UInt32 actualPackLen = rangeOfValues_->pack(target);

  setVCLength(vcLenPtr, vcLenSize, actualPackLen);

  if (sfStats) {
    Int16 filterId = getFilterId();
    long rowsAffected = rangeOfValues_->entries();
    ScanFilterStats stats(filterId, rowsAffected, rangeOfValues_->isEnabled());

    sfStats->addEntry(stats, semantics);
  }

  if (clearWhenDone) rangeOfValues_->clear();
}

ex_expr::exp_return_type ExFunctionRangeOfValues::conditionalPack(ex_clause *clause, atp_struct *atp,
                                                                  OperatorTypeEnum op, const char *msg,
                                                                  NABoolean clearWhenDone, ScanFilterStatsList *sfStats,
                                                                  ScanFilterStats::MergeSemantics semantics) {
#if 0
   fstream& out = getPrintHandle();
   
   if (msg)
      out << msg << endl;

   out << ", Chained ExFunctionRangeOfValues::conditionalPack() for ";
   out << " " << getOperTypeEnumAsString(op) << endl;
#endif

  Attributes *attr = NULL;
  while (clause) {
    if (clause->getOperType() == op) {
      AttributesPtr *op = clause->getOperand();

      // Get info about the result
      const UInt16 atpix = (*op)->getAtpIndex();
      Int16 dataType = (*op)->getDatatype();

      if (dataType != REC_BYTE_V_ASCII) return ex_expr::EXPR_ERROR;

      char *dataPtr = NULL;
      char *target = NULL;
      char *vardata = NULL;
      int size = 0;
      int vcLenSize = 0;

      switch ((*op)->getTupleFormat()) {
        case ExpTupleDesc::SQLARK_EXPLODED_FORMAT: {
          dataPtr = (atp->getTupp(atpix)).getDataPointer();
          target = dataPtr + (*op)->getOffset();
          size = (*op)->getLength();

          vcLenSize = (*op)->getVCIndicatorLength();
          vardata = dataPtr + (*op)->getVCLenIndOffset();
        } break;

        case ExpTupleDesc::SQLMX_FORMAT:
        case ExpTupleDesc::SQLMX_ALIGNED_FORMAT:
        case ExpTupleDesc::SQLMX_KEY_FORMAT:
        case ExpTupleDesc::PACKED_FORMAT:
        default:
          break;
      }

      if (!target) return ex_expr::EXPR_ERROR;

      ((ExFunctionRangeOfValues *)clause)->packRanges(target, vardata, vcLenSize, clearWhenDone, sfStats, semantics);
    }

    clause = clause->getNextClause();
  }

#if 0
   out << "Chained ExFunctionRangeOfValues::conditionalPack() done";
   out << endl;
   out.close();
#endif

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExFunctionRangeOfValues::init() {
  rangeOfValues_->clear();
  return ex_expr::EXPR_OK;
}

void ExFunctionRangeOfValues::sanityCheck() {
  if (rangeOfValues_->isEnabled()) {
    if (!(rangeOfValues_->isUseful(maxNumEntries_))) {
      rangeOfValues_->setEnable(FALSE);
      rangeOfValues_->clear();
    }
  }
}

ex_expr::exp_return_type ExFunctionRangeOfValues::conditionalSanityCheck(ex_clause *clause, OperatorTypeEnum op) {
  Attributes *attr = NULL;
  while (clause) {
    if (clause->getOperType() == op && ExFunctionRangeOfValues::IsRangeOfValuesEnum(op)) {
      ((ExFunctionRangeOfValues *)clause)->sanityCheck();
    }

    clause = clause->getNextClause();
  }

  return ex_expr::EXPR_OK;
}

void ExFunctionRangeOfValues::conditionalDisplay(ex_clause *cptr, OperatorTypeEnum op, const char *msg) {
  fstream &out = getPrintHandle();
  conditionalPrint(out, cptr, op, msg);
  out.close();
}

void ExFunctionRangeOfValues::conditionalPrint(ostream &out, ex_clause *cptr, OperatorTypeEnum op, const char *msg) {
  if (msg) out << msg << endl;

  out << "Chained ExFunctionRangeOfValues::conditionalDisplay() for ";
  out << getOperTypeEnumAsString(op) << endl;

  while (cptr) {
    if (cptr->getOperType() == op && ExFunctionRangeOfValues::IsRangeOfValuesEnum(op)) {
      ExFunctionRangeOfValues *rvFunc = (ExFunctionRangeOfValues *)cptr;
      rvFunc->get()->dump(out);
    }

    cptr = cptr->getNextClause();
  }
}

// show RS addresses
void ExFunctionRangeOfValues::showRSA(ex_clause *cptr, OperatorTypeEnum op, const char *msg) {
  while (cptr) {
    if (cptr->getOperType() == op && ExFunctionRangeOfValues::IsRangeOfValuesEnum(op)) {
      ExFunctionRangeOfValues *rvFunc = (ExFunctionRangeOfValues *)cptr;

      RangeOfValues *rv = rvFunc->get();

      if (msg) cout << msg << ",";

      cout << "@RangeOfValues=" << static_cast<void *>(rv) << endl;
    }

    cptr = cptr->getNextClause();
  }
}
