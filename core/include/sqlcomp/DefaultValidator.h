
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         DefaultValidator.h
 * Description:  Validation of Defaults values.
 *
 *
 *****************************************************************************
 */

#ifndef _NADEFAULTVALIDATOR_H
#define _NADEFAULTVALIDATOR_H

#include <assert.h>
#include <float.h>
#include <limits.h>

#include "common/BaseTypes.h"
#include "common/charinfo.h"
#include "sqlcomp/NADefaults.h"

class SchemaDB;

enum CaseSensitivity { CASE_INSENSITIVE = 0, CASE_SENSITIVE, CASE_SENSITIVE_ANSI };

enum DefaultValidatorType {
  VALID_ANY,
  VALID_CS_ANSI,
  VALID_KWD,
  VALID_INT,
  VALID_UINT,
  VALID_FLT,
  VALID_LINUXDISKS

};

class DefaultValidator {
 public:
  DefaultValidator() : type_(VALID_ANY), caseSensitivity_(CASE_SENSITIVE) {}
  DefaultValidator(DefaultValidatorType vt, CaseSensitivity cs) : type_(vt), caseSensitivity_(cs) {}
  DefaultValidator(CaseSensitivity cs) : type_(VALID_ANY), caseSensitivity_(cs) {}

  DefaultValidatorType getType() const { return type_; }
  NAString getTypeText() const { return getTypeText(type_); }
  static NAString getTypeText(DefaultValidatorType vt);

  CaseSensitivity caseSensitive() const { return caseSensitivity_; }
  void applyUpper(NAString &value) const;

  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;

 protected:
  const DefaultValidatorType type_;
  const CaseSensitivity caseSensitivity_;
};

class ValidateKeyword : public DefaultValidator {
 public:
  ValidateKeyword() : DefaultValidator(VALID_KWD, CASE_INSENSITIVE) {}

  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;
};

class ValidateCollationList : public DefaultValidator {
 public:
  ValidateCollationList(NABoolean formatNSK = FALSE)
      : DefaultValidator(VALID_CS_ANSI, CASE_SENSITIVE_ANSI),
        formatNSK_(formatNSK),
        formatRetry_(FALSE),
        cdb_(NULL),
        sdb_(NULL),
        cntCoParsed_(0),
        lastCoInserted_(CharInfo::UNKNOWN_COLLATION) {}

  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;

  UInt32 insertIntoCDB(SchemaDB *sdb,     // Returns count of collations
                       CollationDB *cdb,  // that it parsed & inserted
                       const char *value, NABoolean formatNSK = FALSE);

  UInt32 cntCoParsed() const { return cntCoParsed_; }
  CharInfo::Collation lastCoInserted() const { return lastCoInserted_; }

 private:
  NABoolean formatNSK_;
  NABoolean formatRetry_;
  CollationDB *cdb_;
  SchemaDB *sdb_;
  UInt32 cntCoParsed_;
  CharInfo::Collation lastCoInserted_;
};

class ValidateTraceStr : public DefaultValidator {
 public:
  // returns FALSE if invalid
  virtual int validate(const char *value, const NADefaults *nad, int attrEnum, int errOrWarn = -1,
                       float *flt = NULL) const;
};

class ValidateAnsiList : public DefaultValidator {
 public:
  ValidateAnsiList() : DefaultValidator() {}  // use VALID_ANY

  virtual NABoolean caseSensitive() const { return FALSE; }
  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;
};

class ValidateRoleNameList : public DefaultValidator  // based on ValidateAnsiList
{
 public:
  ValidateRoleNameList() : DefaultValidator() {}  // use VALID_ANY

  virtual NABoolean caseSensitive() const { return FALSE; }
  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;
};

class ValidatePOSTableSizes : public DefaultValidator {
 public:
  ValidatePOSTableSizes() : DefaultValidator(VALID_ANY, CASE_INSENSITIVE) {}

  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;
};

class ValidateNumericRange : public DefaultValidator {
 public:
  ValidateNumericRange(DefaultValidatorType vt, float mn, float mx)
      : DefaultValidator(vt, CASE_INSENSITIVE), min_(mn), max_(mx), multiple_(0) {}

  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;

  // All numerics are expressed as floats.  Thus there is loss of precision at
  // the high end, which results in things like long(float(LONG_MAX)) printing
  // out as 0.  Staying below this "smear" fence avoids the display problem.
  //	(Note: using enum's for namespace/scoping reasons.
  //	Unfortunately, can't use enum for UINT:  enum's are int's -- SIGNED.)
  enum { INT_TO_FLT_SMEAR = 150 };
  enum {
    INTmin_ = INT_MIN + INT_TO_FLT_SMEAR,
    INTmax_ = INT_MAX - INT_TO_FLT_SMEAR,
  };
#define UINTmax_ UINT_MAX - 2 * INT_TO_FLT_SMEAR
 protected:
  const float min_;
  const float max_;
  size_t multiple_;
};

class ValidateInt : public ValidateNumericRange {
 public:
  ValidateInt() : ValidateNumericRange(VALID_INT, (float)INTmin_, (float)INTmax_) {}
};

class ValidateIntNeg1 : public ValidateNumericRange {
 public:
  ValidateIntNeg1() : ValidateNumericRange(VALID_INT, (float)-2, (float)INTmax_) {}
};

// For unknown reasons, there are many CQDs that are defined to be unsinged
// values (e.g. DDui1, DDui1) and then used as signed values in various places.
// Due to this type mismatch, compiler produces an error when very large values
// are specified. The error messages in ValidateNumericRange::validate() are
// misleading and the suggested range in the error message varies depending on
// the cause and the location. It also makes CQD reset impossible in such cases.
// To avoid these issues, the maximum value for an unsigned CQD is now changed
// to the maximum value of a signed CQD (UINTmax_ => INTmax_) instead of
// modifying the sensitive code where these CQDs are used as signed values.
// Assumption: We will never need a CQD with values upto 4 billion.
class ValidateUInt : public ValidateNumericRange {
 public:
  ValidateUInt() : ValidateNumericRange(VALID_UINT, 0, (float)INTmax_) {}
};

class ValidatePercent : public ValidateNumericRange {
 public:
  ValidatePercent() : ValidateNumericRange(VALID_UINT, 0, (float)100) {}
};

class Validate_0_255 : public ValidateNumericRange {
 public:
  Validate_0_255() : ValidateNumericRange(VALID_UINT, 0, (float)255) {}
};

class Validate_0_200000 : public ValidateNumericRange {
 public:
  Validate_0_200000() : ValidateNumericRange(VALID_UINT, 0, (float)200000) {}
};

class Validate_0_400000 : public ValidateNumericRange {
 public:
  Validate_0_400000() : ValidateNumericRange(VALID_UINT, 0, (float)400000) {}
};

class Validate_0_524288 : public ValidateNumericRange {
 public:
  Validate_0_524288() : ValidateNumericRange(VALID_UINT, 0, (float)524288) {}
};

class Validate_1_200000 : public ValidateNumericRange {
 public:
  Validate_1_200000() : ValidateNumericRange(VALID_UINT, 1, (float)200000) {}
};

class Validate_0_10485760 : public ValidateNumericRange {
 public:
  Validate_0_10485760() : ValidateNumericRange(VALID_UINT, 0, (float)10485760) {}
};

class Validate_30_32000 : public ValidateNumericRange {
 public:
  Validate_30_32000() : ValidateNumericRange(VALID_UINT, (float)30, (float)32000) {}
};

class Validate_30_246 : public ValidateNumericRange {
 public:
  Validate_30_246() : ValidateNumericRange(VALID_UINT, (float)30, (float)246) {}
};

class Validate_50_4194303 : public ValidateNumericRange {
 public:
  Validate_50_4194303() : ValidateNumericRange(VALID_UINT, (float)50, (float)4194303) {}
};

class Validate_1_24 : public ValidateNumericRange {
 public:
  Validate_1_24() : ValidateNumericRange(VALID_UINT, 1, (float)24) {}
};

// For unknown reasons, there are many CQDs that are defined to be unsinged
// values (e.g. DDui1, DDui1) and then used as signed values in various places.
// Due to this type mismatch, compiler produces an error when very large values
// are specified. The error messages in ValidateNumericRange::validate() are
// misleading and the suggested range in the error message varies depending on
// the cause and the location. It also makes CQD reset impossible in such cases.
// To avoid these issues, the maximum value for an unsigned CQD is now changed
// to the maximum value of a signed CQD (UINTmax_ => INTmax_) instead of
// modifying the sensitive code where these CQDs are used as signed values.
// Assumption: We will never need a CQD with values upto 4 billion.
class ValidateUInt1 : public ValidateNumericRange {
 public:
  ValidateUInt1() : ValidateNumericRange(VALID_UINT, 1, (float)INTmax_) {}
};

class ValidateUInt2 : public ValidateUInt1 {
 public:
  ValidateUInt2(size_t multiple = 2) : ValidateUInt1() { multiple_ = multiple; }
};

//----------------------------------------------------------------------------
//++MV OZ
class ValidateUIntFrom0To2 : public ValidateNumericRange {
 public:
  ValidateUIntFrom0To2() : ValidateNumericRange(VALID_UINT, (float)0, (float)2) {}
};

class ValidateUIntFrom0To5 : public ValidateNumericRange {
 public:
  ValidateUIntFrom0To5() : ValidateNumericRange(VALID_UINT, (float)0, (float)5) {}
};

class ValidateUIntFrom1To6 : public ValidateNumericRange {
 public:
  ValidateUIntFrom1To6() : ValidateNumericRange(VALID_UINT, (float)1, (float)6) {}
};

class ValidateUIntFrom1To8 : public ValidateNumericRange {
 public:
  ValidateUIntFrom1To8() : ValidateNumericRange(VALID_UINT, (float)1, (float)8) {}
};

class ValidateUIntFrom1To10 : public ValidateNumericRange {
 public:
  ValidateUIntFrom1To10() : ValidateNumericRange(VALID_UINT, (float)1, (float)10) {}
};

class ValidateUIntFrom2To10 : public ValidateNumericRange {
 public:
  ValidateUIntFrom2To10() : ValidateNumericRange(VALID_UINT, (float)2, (float)10) {}
};

class ValidateUIntFrom1500To4000 : public ValidateNumericRange {
 public:
  ValidateUIntFrom1500To4000() : ValidateNumericRange(VALID_UINT, (float)1500, (float)4000) {}
};

class ValidateUIntForOptimizedScan : public ValidateNumericRange {
 public:
  ValidateUIntForOptimizedScan();
};

// We say 54K here to allow a safe overhead in the 56K local-DP2 msg buf size
class ValidateIPCBuf : public ValidateNumericRange {
 public:
  ValidateIPCBuf(size_t multiple = 8) : ValidateNumericRange(VALID_UINT, 0, (float)(54 * 1024)) {
    multiple_ = multiple;
  }
};

class ValidateFlt : public ValidateNumericRange {
 public:
  ValidateFlt() : ValidateNumericRange(VALID_FLT, -FLT_MAX, FLT_MAX) {}
};

class ValidateFltMin0 : public ValidateNumericRange {
 public:
  ValidateFltMin0() : ValidateNumericRange(VALID_FLT, 0, FLT_MAX) {}
};

class ValidateFltMinEpsilon : public ValidateNumericRange {
 public:
  ValidateFltMinEpsilon() : ValidateNumericRange(VALID_FLT, FLT_MIN, FLT_MAX) {}
};

class ValidateFltMin1 : public ValidateNumericRange {
 public:
  ValidateFltMin1() : ValidateNumericRange(VALID_FLT, 1.0, FLT_MAX) {}
};

class ValidateSelectivity : public ValidateNumericRange {
 public:
  // restrict the minimum value for selectivity to 1e-10, which is
  // equivalent to COSTSCALAR_EPSILON (defined in CostScalar.h)
  ValidateSelectivity() : ValidateNumericRange(VALID_FLT, (float)1e-10, 1.0) {}
};
class ValidateFlt_0_1 : public ValidateNumericRange {
 public:
  ValidateFlt_0_1() : ValidateNumericRange(VALID_FLT, 0.0, 1.0) {}
};

class Validate_1_4096 : public ValidateNumericRange {
 public:
  Validate_1_4096() : ValidateNumericRange(VALID_UINT, 1, (float)4096) {}
};

class Validate_1_8192 : public ValidateNumericRange {
 public:
  Validate_1_8192() : ValidateNumericRange(VALID_UINT, 1, (float)8192) {}
};

class Validate_1_40000 : public ValidateNumericRange {
 public:
  Validate_1_40000() : ValidateNumericRange(VALID_UINT, 1, (float)40000) {}
};

class Validate_0_18 : public ValidateNumericRange {
 public:
  Validate_0_18() : ValidateNumericRange(VALID_UINT, 0, (float)18) {}
};

class Validate_0_64 : public ValidateNumericRange {
 public:
  Validate_0_64() : ValidateNumericRange(VALID_UINT, 0, (float)64) {}
};

class Validate_16_64 : public ValidateNumericRange {
 public:
  Validate_16_64() : ValidateNumericRange(VALID_UINT, 16, (float)64) {}
};

class Validate_1_1024 : public ValidateNumericRange {
 public:
  Validate_1_1024() : ValidateNumericRange(VALID_UINT, 1, (float)1024) {}
};

class Validate_18_128 : public ValidateNumericRange {
 public:
  Validate_18_128() : ValidateNumericRange(VALID_UINT, 18, (float)128) {}
};

class Validate_1_128 : public ValidateNumericRange {
 public:
  Validate_1_128() : ValidateNumericRange(VALID_UINT, 1, (float)128) {}
};

class Validate_uint16 : public ValidateNumericRange {
 public:
  Validate_uint16() : ValidateNumericRange(VALID_UINT, 1, (float)USHRT_MAX) {}
};

class ValidateOverrideSchema : public DefaultValidator {
 public:
  ValidateOverrideSchema() : DefaultValidator() {}

  virtual int validate(const char *value, const NADefaults *nad, int attrEnum, int errOrWarn = -1,
                       float *flt = NULL) const;
};

class ValidatePublicSchema : public DefaultValidator {
 public:
  ValidatePublicSchema() : DefaultValidator() {}

  virtual int validate(const char *value, const NADefaults *nad, int attrEnum, int errOrWarn = -1,
                       float *flt = NULL) const;
};

class ValidateReplIoVersion : public DefaultValidator {
 public:
  ValidateReplIoVersion(int minReplIoVersion, int maxReplIoVersion) : DefaultValidator() {
    min_ = minReplIoVersion;
    max_ = maxReplIoVersion;
  }

  virtual int validate(const char *value, const NADefaults *nad, int attrEnum, int errOrWarn = -1,
                       float *flt = NULL) const;

 protected:
  int max_;
  int min_;
};

class ValidateMVAge : public DefaultValidator {
 public:
  ValidateMVAge() : DefaultValidator() {}  // use VALID_ANY

  virtual NABoolean caseSensitive() const { return FALSE; }
  virtual int validate(const char *value,  // returns FALSE if invalid
                       const NADefaults *nad, int attrEnum, int errOrWarn = -1, float *flt = NULL) const;
};

#endif /* _NADEFAULTVALIDATOR_H */
