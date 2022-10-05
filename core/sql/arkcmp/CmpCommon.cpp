
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpCommon.C
 * Description:  The implementation of CmpCommon class, which includes the
 *               static functions to get the contents of the CmpContext
 *               class (containing the global info for arkcmp).
 *
 *
 * Created:      09/05/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include <stdio.h>
#include <iostream>
#include <string>

#include "common/Platform.h"

#include "common/CmpCommon.h"
#include "arkcmp/CmpContext.h"
#include "arkcmp/CmpErrors.h"
#include "arkcmp/CmpStatement.h"
#include "sqlmsg/ErrorMessage.h"

#include "sqlmxevents/logmxevent.h"

#include "sqlcomp/NADefaults.h"
#include "sqlcomp/NewDel.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/ControlDB.h"
#include "arkcmp/CompException.h"

#include "NAInternalError.h"

#include "cli/Context.h"
#include "cli/Globals.h"
#include "dbsecurity/dbUserAuth.h"

THREAD_P CmpContext *cmpCurrentContext = 0;
THREAD_P jmp_buf CmpInternalErrorJmpBuf;

//
// Two jmp_buf pointers that the compiler will longjmp from.
// These two pointers must be assigned to the addresss of the
// jmp buffer when setjmp() is called when entering into the
// compiler.
//
THREAD_P jmp_buf *CmpInternalErrorJmpBufPtr;
THREAD_P jmp_buf *ExportJmpBufPtr;

static void readEnvFromFile(const char *filename, const char *key, char *value, size_t valLen);

#define FIND_KEY_IN_MS_ENV(KEY, VALUE, VAL_LEN)             \
  {                                                         \
    const char *traf_conf = getenv("TRAF_VAR");             \
    if (NULL != traf_conf && 0 != traf_conf[0]) {           \
      char msenv_filename[255] = {0};                       \
      sprintf(msenv_filename, "%s/ms.env", traf_conf);      \
      readEnvFromFile(msenv_filename, KEY, VALUE, VAL_LEN); \
    }                                                       \
  }

ComDiagsArea *CmpCommon::diags() { return (cmpCurrentContext ? cmpCurrentContext->diags() : 0); }

CmpContext *CmpCommon::context() { return cmpCurrentContext; }

CmpStatement *CmpCommon::statement() { return (cmpCurrentContext ? cmpCurrentContext->statement() : 0); }

CollHeap *CmpCommon::contextHeap() { return (cmpCurrentContext ? cmpCurrentContext->heap() : 0); }

NAHeap *CmpCommon::statementHeap() { return (statement() ? statement()->heap() : 0); }

TransMode *CmpCommon::transMode() {
  TransMode &transMode = cmpCurrentContext->getTransMode();

  if (transMode.isolationLevel() == TransMode::IL_NOT_SPECIFIED_ && ActiveSchemaDB()) {
    // First time in, *after* CmpCommon/CmpContext/SchemaDB has been init'd
    // (Genesis 10-990224-2929); cf. SchemaDB::initPerStatement for more info.
    // Get current NADefaults setting of ISOLATION_LEVEL --
    // if invalid, use Ansi default of serializable -- then set our global.
    ActiveSchemaDB()->getDefaults().getIsolationLevel(transMode.isolationLevel());
    CMPASSERT(transMode.isolationLevel() != TransMode::IL_NOT_SPECIFIED_);
  }
  return &(cmpCurrentContext->getTransMode());
}

const NAString *CmpCommon::getControlSessionValue(const NAString &token) {
  return ActiveControlDB()->getControlSessionValue(token);
}

// *********** About Error Handling in CmpAssertInternal ****************
// 1. TFDS must be done here to get a complete statck trace,
//    It is only enabled with a special control query default.
//    See the implemenation of makeTFDSCall().
// 3. Debugging support must be done here to give a complete stack trace.
// 3. Logging of EMS event is done in the outermost catch handler.
// 4. Adding Error/Warning to ComDiagsArea is done in the outermost catch
//    handler.
// **********************************************************************
void CmpAssertInternal(const char *condition, const char *file, int num) {
  NAInternalError::throwAssertException(condition, file, num);

  CmpInternalException(condition, file, num).throwException();
}

void CmpAbortInternal(const char *msg, const char *file, int num) {
  NAInternalError::throwFatalException(msg, file, num);

  throw EHBreakException(file, num);
}

void CmpCommon::dumpDiags(ostream &outStream, NABoolean newline) { NADumpDiags(outStream, diags(), newline); }

// The following functions allow access to the defaults table
// in sqlcomp/NADefaults.cpp; given the id number of the default, it
// returns the value
int CmpCommon::getDefaultLong(DefaultConstants id) { return ActiveSchemaDB()->getDefaults().getAsLong(id); }

double CmpCommon::getDefaultNumeric(DefaultConstants id) {
  float result;

  if (!ActiveSchemaDB()->getDefaults().getFloat(id, result)) {
    CMPASSERT("CmpCommon::getDefaultNumeric()" == NULL);
  }

  return result;
}

NAString CmpCommon::getDefaultString(DefaultConstants id) { return ActiveSchemaDB()->getDefaults().getString(id); }

DefaultToken CmpCommon::getDefault(DefaultConstants id, int errOrWarn) {
  return ActiveSchemaDB()->getDefaults().getToken(id, errOrWarn);
}

DefaultToken CmpCommon::getDefault(DefaultConstants id, NAString &result, int errOrWarn) {
  return ActiveSchemaDB()->getDefaults().token(id, result, FALSE, errOrWarn);
}

NABoolean CmpCommon::wantCharSetInference() {
  NAString defVal;
  return getDefault(INFER_CHARSET, defVal) == DF_ON;
}

void CmpCommon::applyDefaults(ComObjectName &nam) {
  CMPASSERT(nam.isValid());
  if (nam.getCatalogNamePart().isEmpty()) {
    NAString dcat, dsch;
    ActiveSchemaDB()->getDefaults().getCatalogAndSchema(dcat, dsch);

    if (nam.getSchemaNamePart().isEmpty()) nam.setSchemaNamePart(ComAnsiNamePart(dsch, CmpCommon::statementHeap()));
    nam.setCatalogNamePart(ComAnsiNamePart(dcat, CmpCommon::statementHeap()));
  }
}

ComAuthenticationType CmpCommon::loadAuthenticationType() {
  ComAuthenticationType authType;
  char whichType[255] = {0};
  FIND_KEY_IN_MS_ENV("TRAFODION_AUTHENTICATION_TYPE", whichType, 255 - 1);
  // The arguments of strcmp are not allowed be NULL
  if (0 == whichType[0]) {
    // for develop
    return COM_NO_AUTH;
  }
  // LDAP
  if (0 == strcasecmp(whichType, "LDAP")) {
    authType = COM_LDAP_AUTH;
  } else if (0 == strcasecmp(whichType, "SKIP_AUTHENTICATION")) {
    // non-password login
    authType = COM_NO_AUTH;
  }
  // else if (0 == strcasecmp(whichType, "LOCAL"))
  // use default
  else {
    authType = COM_LOCAL_AUTH;
  }
  return authType;
}

#define CONVERT_CHAR_TO_NUMBER(intval, str)                                     \
  {                                                                             \
    intval = 0;                                                                 \
    char num[sizeof(int)] = {0};                                              \
    num[0] = str[i];                                                            \
    num[1] = str[i + 1];                                                        \
    if (isxdigit(num[0]) && isxdigit(num[1])) sscanf(num, "%x", &intval);       \
    intval = (0 == intval && ('0' != num[0] && '0' != num[1])) ? 0xdd : intval; \
    i += 2;                                                                     \
  }

#define CONVERT_POLICY_STRING(intval, str, error) \
  {                                               \
    CONVERT_CHAR_TO_NUMBER(intval, str);          \
    if (0xdd == intval) {                         \
      error = TRUE;                               \
    }                                             \
  }

#define SYNTAX_STRING_LINE_PARSE(syntax, str, error)                \
  {                                                                 \
    size_t i = 0;                                                   \
    CONVERT_POLICY_STRING(syntax.min_password_length, str, error);  \
    CONVERT_POLICY_STRING(syntax.max_password_length, str, error);  \
    CONVERT_POLICY_STRING(syntax.upper_chars_number, str, error);   \
    CONVERT_POLICY_STRING(syntax.lower_chars_number, str, error);   \
    CONVERT_POLICY_STRING(syntax.digital_chars_number, str, error); \
    CONVERT_POLICY_STRING(syntax.symbol_chars_number, str, error);  \
  }

#define VAILD_RANGE_CHECK(num)                                                         \
  {                                                                                    \
    if (num < 0 || (num != 0xee && num != 0xff && num > syntax.max_password_length)) { \
      isInvaild = TRUE;                                                                \
    }                                                                                  \
  }

#define NUMBER_OF_CHAR_COUNT(item)                      \
  {                                                     \
    if (item > 0 && item <= syntax.max_password_length) \
      charNums += item;                                 \
    else if (item == 0)                                 \
      charNums += 1;                                    \
  }

/********************************
 *______________________________________
 *| 0-1 | 2-3 | 4-5 | 6-7 | 8-9 | 10-11 |
 *| min | max |upper|lower| num | -_@./ |
 *---------------------------------------
 *********************************/

static NABoolean CheckPasswordPolicySyntax(ComPwdPolicySyntax &syntax) {
  NABoolean isInvaild = FALSE;
  UInt32 charNums = 0;
  // min == max is ok
  if (syntax.min_password_length > syntax.max_password_length) isInvaild = TRUE;
  // min & max are not 0
  if (0 == syntax.max_password_length || 0 == syntax.min_password_length) isInvaild = TRUE;
  // min & max are less then MAX_PASSWORD_LENGTH
  if (syntax.max_password_length > MAX_PASSWORD_LENGTH || syntax.min_password_length > MAX_PASSWORD_LENGTH)
    isInvaild = TRUE;
  if (isInvaild) {
    goto END;
  }
  // number range check
  VAILD_RANGE_CHECK(syntax.digital_chars_number);
  VAILD_RANGE_CHECK(syntax.lower_chars_number);
  VAILD_RANGE_CHECK(syntax.upper_chars_number);
  VAILD_RANGE_CHECK(syntax.symbol_chars_number);
  if (isInvaild) {
    goto END;
  }
  // no char can uses
  if (syntax.digital_chars_number == 0xee && syntax.lower_chars_number == 0xee && syntax.upper_chars_number == 0xee &&
      syntax.symbol_chars_number == 0xee) {
    isInvaild = TRUE;
    goto END;
  }
  // compute number of chars
  // all of sum of kinds of chars is less than 128
  NUMBER_OF_CHAR_COUNT(syntax.digital_chars_number);
  NUMBER_OF_CHAR_COUNT(syntax.lower_chars_number);
  NUMBER_OF_CHAR_COUNT(syntax.upper_chars_number);
  NUMBER_OF_CHAR_COUNT(syntax.symbol_chars_number);
  if (syntax.digital_chars_number == 0xff && syntax.lower_chars_number == 0xff && syntax.upper_chars_number == 0xff &&
      syntax.symbol_chars_number == 0xff) {
    // reserves 3 chars
    charNums = 3;
  }
  // too long
  if (charNums > syntax.max_password_length) isInvaild = TRUE;
  // too short
  if (charNums > syntax.min_password_length) {
    syntax.min_password_length = charNums;
  }

END:
  return !isInvaild;
}

// see CAT_INVALID_SYNTAX_FORMAT on DefaultConstants.h
#define CAT_INVALID_SYNTAX_FORMAT 1373
NABoolean CmpCommon::ConvertPasswordPolicyString(const char *syntaxStr, ComPwdPolicySyntax &syntax, const char *where,
                                                 bool justWarning) {
  if (syntaxStr && COM_PWD_CHECK_SYNTAX_LEN == strlen(syntaxStr)) {
    if (0 == strcasecmp("FFFFFFFFFFFF", syntaxStr)) {
      // skip password check
      syntax.max_password_length = 0xFF;
      goto END;
    } else if (0 != strcasecmp("000000000000", syntaxStr)) {
      NABoolean isInvalid = FALSE;
      SYNTAX_STRING_LINE_PARSE(syntax, syntaxStr, isInvalid);
      if (!isInvalid) {
        if (CheckPasswordPolicySyntax(syntax)) {
          goto END;
        }
      }
      *CmpCommon::diags() << DgSqlCode(justWarning ? CAT_INVALID_SYNTAX_FORMAT : -CAT_INVALID_SYNTAX_FORMAT)
                          << DgString0(syntaxStr) << DgString1(where);
    } else {
      // not set?
      // no warning
    }
  } else {
    *CmpCommon::diags() << DgSqlCode(justWarning ? CAT_INVALID_SYNTAX_FORMAT : -CAT_INVALID_SYNTAX_FORMAT)
                        << DgString0(syntaxStr) << DgString1(where);
  }
  return FALSE;
END:
  return TRUE;
}

/*
    If a valid password policy syntax is defined on environment variable,
    return a flag and storage the value on cmpContext -- as default value
    Or reading value from default every time. -- can be dynamically changed by CQD
*/
NABoolean CmpCommon::loadPasswordCheckSyntax(ComPwdPolicySyntax &syntax, NABoolean skipEnvVar) {
  char syntaxStr[255] = {0};
  NABoolean validSyntaxFound = FALSE;
  memset((void *)&syntax, 0, sizeof(ComPwdPolicySyntax));
  /*
      [0-1] minimum password length
      [2-3] maximum password length
      [4-5] number of uppercase alphabet
      [6-7] number of lowercase alphabet
      [8-9] number of digital alphabet
      [10-11] number of symbol alphabet

      number of chars
      00    - must use,but no care about numbers of this char
      ee    - must not use
      ff    - no check
      other - numbers of char

      number of password length
      max len - 0xff skip check
      max len - 0    not set
  */
  // Top priority : environment variables
  if (skipEnvVar) {
    goto LOAD_FROM_DEFAULT;
  }
  FIND_KEY_IN_MS_ENV("TRAFODION_PASSWORD_CHECK_SYNTAX", syntaxStr, 255 - 1);
  if (0 == syntaxStr[0]) {
    // skip environment variables
  } else if (TRUE == (validSyntaxFound = ConvertPasswordPolicyString(syntaxStr, syntax, "environment variables"))) {
    goto END;
  }
LOAD_FROM_DEFAULT:
  // Second priority : TRAF_PASSWORD_POLICY_SYNTAX in default table
  {
    NAString naStr = CmpCommon::getDefaultString(TRAF_PASSWORD_POLICY_SYNTAX);
    if (!naStr.isNull()) {
      strncpy(syntaxStr, naStr.data(), sizeof(syntaxStr) - 1);
      if (TRUE == (validSyntaxFound = ConvertPasswordPolicyString(syntaxStr, syntax, "inner default value"))) {
        goto END;
      }
    }
  }
  // Not priority : Internal rules
  /*
  Internal rules:
  length of password:     6~128
  structure of password:  3 different type of character at last.
  */
  {
    memset((void *)&syntax, 0, sizeof(ComPwdPolicySyntax));
    syntax.max_password_length = MAX_PASSWORD_LENGTH;
    syntax.min_password_length = getDefaultLong(MIN_PASSWORD_LENGTH);
    if (syntax.min_password_length > syntax.max_password_length) {
      syntax.min_password_length = syntax.max_password_length;
    }
    syntax.upper_chars_number = 0xFF;
    syntax.lower_chars_number = 0xFF;
    syntax.digital_chars_number = 0xFF;
    syntax.symbol_chars_number = 0xFF;
  }
END:
  if (CmpCommon::diags()->containsWarning(CAT_INVALID_SYNTAX_FORMAT)) {
    // ComDiagsArea will be cleaned when next of statement be compiled.
    QRLogger::initLog4cplus(QRLogger::QRL_MXEXE);
    std::string cat("SQL");
    QRLogger::logDiags(CmpCommon::diags(), cat);
    CmpCommon::diags()->clearWarnings();
  }
  return validSyntaxFound;
}

// get from globals
ComAuthenticationType CmpCommon::getAuthenticationType() { return GetCliGlobals()->getAuthenticationType(); }

ComPwdPolicySyntax CmpCommon::getPasswordCheckSyntax() {
  ComPwdPolicySyntax tmp = {0xff};
  if (COM_LDAP_AUTH == getAuthenticationType()) {
    return tmp;
  }
  if ("ON" == getDefaultString(TRAF_IGNORE_EXTERNAL_PASSWORD_POLICY_SYNTAX)) {
    loadPasswordCheckSyntax(tmp, TRUE);
    return tmp;
  }
  return *(GetCliGlobals()->getPasswordCheckSyntax());
}

// save to globals
void CmpCommon::setPasswordCheckSyntax(ComPwdPolicySyntax pwdPolicy) {
  GetCliGlobals()->setPasswordCheckSyntax(pwdPolicy);
}
void CmpCommon::setAuthenticationType(ComAuthenticationType authType) {
  GetCliGlobals()->setAuthenticationType(authType);
  DBUserAuth::setAuthenticationType((int)authType);
}
NABoolean CmpCommon::isUninitializedSeabase() { return CmpCommon::context()->isUninitializedSeabase(); }

static void string_trim(std::string &s) {
  if (s.empty()) {
    return;
  }
  s.erase(0, s.find_first_not_of(" "));
  s.erase(s.find_last_not_of(" ") + 1);
}

static void readEnvFromFile(const char *fileName, const char *key, char *value, size_t len) {
  std::string str;
  std::string oneline;
  std::ifstream inFile(fileName, ios::in);
  if (!inFile) {
    value[0] = 0;
    return;
  }
  while (getline(inFile, oneline)) {
    string_trim(oneline);
    // k=v
    std::size_t pos = oneline.find(key, 0, strlen(key));
    if (std::string::npos != pos) {
      pos = oneline.find_first_of("=", pos);
      if (std::string::npos != pos) {
        str = oneline.substr(pos + 1);
        string_trim(str);
        // break; no break for avoid to duplicate lines
      }
    }
  }
  inFile.close();
  if (!str.empty()) {
    strncpy(value, str.c_str(), len);
  } else {
    value[0] = 0;
  }
}

#define INIT_QI_KEY(qikey, subjectUserID, objectUID, which) \
  {                                                         \
    ComSecurityKey secKey(subjectUserID, objectUID, which); \
    qikey.revokeKey.subject = secKey.getSubjectHashValue(); \
    qikey.revokeKey.object = secKey.getObjectHashValue();   \
    std::string actionString;                               \
    secKey.getSecurityKeyTypeAsLit(actionString);           \
    strncpy(qikey.operation, actionString.c_str(), 2);      \
  }

// for statement 'alter user xxx add/remove groups' or 'alter group xxx add/remove members'
// to invalidate the query cache for re-compile statement next time to recheck the role from user
void CmpCommon::letsQuerycacheToInvalidated(std::vector<int32_t> &authIDs, std::vector<int32_t> &roleIDs) {
  std::vector<SQL_QIKEY> siKeyList;
  size_t siIndex = 0;
  for (int i = 0; i < authIDs.size(); i++) {
    for (int j = 0; j < roleIDs.size(); j++) {
      SQL_QIKEY qiKey;
      INIT_QI_KEY(qiKey, authIDs[i], roleIDs[j], ComSecurityKey::SUBJECT_IS_USER);
      siKeyList.push_back(qiKey);
      siIndex++;
    }
  }
  if (!siKeyList.empty()) {
    // Call the CLI to send details to RMS
    SQL_EXEC_SetSecInvalidKeys(siIndex, &siKeyList[0]);
  }
}

void CmpCommon::getDifferenceWithTwoVector(std::vector<int32_t> &A, std::vector<int32_t> &B, std::vector<int32_t> &C) {
  C.clear();
  if (A.empty()) {
    return;
  }
  C.resize(A.size() > B.size() ? A.size() : B.size());
  std::sort(A.begin(), A.end());
  std::sort(B.begin(), B.end());
  std::set_difference(A.begin(), A.end(), B.begin(), B.end(), C.begin());
  // remove unused items
  for (std::vector<int32_t>::iterator itor = C.begin(); itor != C.end();) {
    if (0 == *itor) {
      itor = C.erase(itor);
    } else {
      itor++;
    }
  }
}

bool CmpCommon::isModuleOpen(int ModuleId) { return true; }
