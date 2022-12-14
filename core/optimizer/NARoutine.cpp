//**********************************************************************

/**********************************************************************/
/* -*-C++-*-
**************************************************************************
*
* File:         NAroutine.cpp
* Description:  SQL compiler representation of  SP routine metadata
* Created:      5/3/2000
* Language:     C++
*
*
**************************************************************************
*/
#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's

#include "optimizer/NARoutine.h"

#include "arkcmp/NATableSt.h"
#include "cli/Context.h"
#include "cli/Globals.h"
#include "common/BaseTypes.h"
#include "common/CharType.h"
#include "common/CmpCommon.h"
#include "common/ComSmallDefs.h"
#include "common/ComUser.h"
#include "common/DatetimeType.h"
#include "common/NAType.h"
#include "common/NumericType.h"
#include "common/str.h"
#include "langman/LmJavaSignature.h"
#include "optimizer/BindWA.h"
#include "optimizer/NARoutineDB.h"
#include "optimizer/SchemaDB.h"
#include "optimizer/UdrErrors.h"
#include "smdio/CmUtil.h"
#include "sqlcat/TrafDDLdesc.h"
#include "sqlcomp/CmpMain.h"
#include "sqlcomp/CmpSeabaseDDL.h"

#define SQLPARSERGLOBALS_NADEFAULTS
#include "parser/SqlParserGlobals.h"  // should be last #include
#include "parser/SqlParserGlobalsCmn.h"

// -----------------------------------------------------------------------
// Copy a string.  A null terminated buffer is returned.
// -----------------------------------------------------------------------
char *UDRCopyString(const ComString &sourceString, CollHeap *heap) {
  char *string = new (heap) char[sourceString.length() + 1];
  str_cpy_all(string, sourceString.data(), sourceString.length() + 1);
  return string;
}  // UDRCopyString

NARoutine::NARoutine(CollHeap *heap)
    : name_("", heap),
      extRoutineName_(NULL)  // ExtendedQualName *
      ,
      extActionName_(NULL)  // ExtendedQualName * - Empty if not an action.
      ,
      intActionName_(NULL)  // ComObjectName * - Empty if not an action.
      ,
      language_(COM_UNKNOWN_ROUTINE_LANGUAGE),
      UDRType_(COM_UNKNOWN_ROUTINE_TYPE),
      sqlAccess_(COM_UNKNOWN_ROUTINE_SQL_ACCESS),
      transactionAttributes_(COM_UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE),
      maxResults_(0),
      stateAreaSize_(0),
      externalFile_("", heap),
      externalPath_("", heap),
      externalName_("", heap),
      librarySqlName_(NULL),
      signature_("", heap),
      paramStyle_(COM_UNKNOWN_ROUTINE_PARAM_STYLE),
      paramStyleVersion_(1),
      isDeterministic_(FALSE),
      isCallOnNull_(TRUE),
      isIsolate_(TRUE),
      externalSecurity_(COM_ROUTINE_EXTERNAL_SECURITY_INVOKER),
      isExtraCall_(FALSE),
      hasOutParams_(FALSE),
      redefTime_(0),
      lastUsedTime_(0),
      secKeySet_(heap),
      passThruDataNumEntries_(0),
      passThruData_(NULL),
      passThruDataSize_(NULL),
      udfFanOut_(1),
      uecValues_(heap, 0),
      isUniversal_(FALSE),
      actionPosition_(-1),
      executionMode_(COM_ROUTINE_SAFE_EXECUTION),
      objectUID_(0),
      dllName_("", heap),
      dllEntryPoint_("", heap),
      comRoutineParallelism_("NO", heap),
      sasFormatWidth_("", heap),
      systemName_("", heap),
      dataSource_("", heap),
      fileSuffix_("", heap),
      schemaVersionOfRoutine_(COM_VERS_UNKNOWN),
      libRedefTime_(0),
      libBlobHandle_("", heap),
      libSchName_("", heap),
      libVersion_(1),
      libObjUID_(0),
      objectOwner_(0),
      schemaOwner_(0),
      privInfo_(NULL),
      privDescs_(NULL),
      heap_(heap) {}

// This is an empty constructor, with only the name specified
// It is meant for use with TMUDF code where we want to fake the
// presence of a TMUDF in metadata, for example with predefined
// table mapping functions, where the metadata is all determined
// by the compiler interface.

NARoutine::NARoutine(const QualifiedName &name, CollHeap *heap)
    : name_(name, heap),
      hashKey_(name, heap),
      language_(COM_LANGUAGE_C),
      UDRType_(COM_TABLE_UDF_TYPE),
      sqlAccess_(COM_NO_SQL),
      transactionAttributes_(COM_UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE),
      maxResults_(0),
      stateAreaSize_(0),
      externalFile_("", heap),
      externalPath_("", heap),
      externalName_("", heap),
      librarySqlName_(NULL),
      libRedefTime_(-1),
      libBlobHandle_("", heap),
      libSchName_("", heap),
      libVersion_(1),
      libObjUID_(0),
      signature_("", heap),
      paramStyle_(COM_STYLE_SQLROW),
      paramStyleVersion_(COM_ROUTINE_PARAM_STYLE_VERSION_1),
      isDeterministic_(1),
      isCallOnNull_(1),
      isIsolate_(0),
      externalSecurity_(COM_ROUTINE_EXTERNAL_SECURITY_INVOKER),
      isExtraCall_(1),
      hasOutParams_(FALSE),
      redefTime_(0),
      lastUsedTime_(0),
      secKeySet_(heap),
      passThruDataNumEntries_(0),
      passThruData_(NULL),
      passThruDataSize_(NULL),
      udfFanOut_(1),
      uecValues_(heap, 0),
      isUniversal_(0),
      actionPosition_(-1),
      executionMode_(COM_ROUTINE_SAFE_EXECUTION),
      objectUID_(0),
      dllName_("", heap),
      dllEntryPoint_("", heap),
      comRoutineParallelism_("", heap),
      sasFormatWidth_("", heap),
      systemName_("", heap),
      dataSource_("", heap),
      fileSuffix_("", heap),
      schemaVersionOfRoutine_(COM_VERS_2500),
      objectOwner_(0),
      schemaOwner_(0),
      privInfo_(NULL),
      privDescs_(NULL),
      heap_(heap) {
  CollIndex colCount = 0;
  NAColumn *newCol = NULL;
  NAType *newColType = NULL;
  extRoutineName_ = new (heap) ExtendedQualName(name_, heap);
  extActionName_ = new (heap) NAString(heap);
  intActionName_ = new (heap) ComObjectName(heap);
  params_ = new (heap) NAColumnArray(heap);
  inParams_ = new (heap) NAColumnArray(heap);
  outParams_ = new (heap) NAColumnArray(heap);

  // Construct the CostVectors
  // CQDs are checked at Bind time.
  int initCpuCost = -1;
  int initIOCost = -1;
  int initMsgCost = -1;

  CostScalar initialCpuCost(initCpuCost);
  CostScalar initialIOCost(initIOCost);
  CostScalar initialMsgCost(initMsgCost);

  initialRowCost_.setCPUTime(initCpuCost < 0 ? csMinusOne : initialCpuCost);
  initialRowCost_.setIOTime(initIOCost < 0 ? csMinusOne : initialIOCost);
  initialRowCost_.setMSGTime(initMsgCost < 0 ? csMinusOne : initialMsgCost);

  int normCpuCost = -1;
  int normIOCost = -1;
  int normMsgCost = -1;

  CostScalar normalCpuCost(normCpuCost);
  CostScalar normalIOCost(normIOCost);
  CostScalar normalMsgCost(normMsgCost);

  normalRowCost_.setCPUTime(normCpuCost < 0 ? csMinusOne : normalCpuCost);
  normalRowCost_.setIOTime(normIOCost < 0 ? csMinusOne : normalIOCost);
  normalRowCost_.setMSGTime(normMsgCost < 0 ? csMinusOne : normalMsgCost);

  // this is for prototyping only
  for (CollIndex currentCol = 0; currentCol < colCount; currentCol++) {
    // Create the new NAType.
    newColType =
        new (heap) SQLVarChar(heap, 255, 1, FALSE, FALSE /*not caseinsensitive, for now*/
                              ,
                              CharInfo::ISO88591, CharInfo::DefaultCollation, CharInfo::COERCIBLE, CharInfo::ISO88591);

    ComColumnDirection colDirection = COM_INPUT_COLUMN;

    NAString nameStr("pattern");
    NAString empStr(" ");

    // Create the new NAColumn and insert it into the NAColumnArray
    newCol = new (heap) NAColumn((char *)nameStr.data(), currentCol + 1, newColType, heap, NULL  // NATable *
                                 ,
                                 USER_COLUMN, COM_NO_DEFAULT, NULL  // default value
                                 ,
                                 NULL, FALSE, FALSE  // addedColumn
                                 ,
                                 colDirection, FALSE, NULL  // (char *) ucol->routineParamType
    );

    inParams_->insert(newCol);
    params_->insert(newCol);
  }  // for

  passThruDataNumEntries_ = 0;
  passThruData_ = NULL;
  passThruDataSize_ = NULL;

  // Copy privilege information
  heapSize_ = (heap ? heap->getTotalSize() : 0);
}  // NARoutine

NARoutine::NARoutine(const NARoutine &old, CollHeap *h)
    : name_(old.name_, h),
      hashKey_(old.hashKey_, h),
      language_(old.language_),
      UDRType_(old.UDRType_),
      sqlAccess_(old.sqlAccess_),
      transactionAttributes_(old.transactionAttributes_),
      maxResults_(old.maxResults_),
      externalFile_(old.externalFile_, h),
      externalPath_(old.externalPath_, h),
      externalName_(old.externalName_, h),
      signature_(old.signature_, h),
      librarySqlName_(old.librarySqlName_, h),
      libRedefTime_(old.libRedefTime_),
      libBlobHandle_(old.libBlobHandle_, h),
      libSchName_(old.libSchName_, h),
      libVersion_(old.libVersion_),
      libObjUID_(old.libObjUID_),
      paramStyle_(old.paramStyle_),
      paramStyleVersion_(old.paramStyleVersion_),
      isDeterministic_(old.isDeterministic_),
      isCallOnNull_(old.isCallOnNull_),
      isIsolate_(old.isIsolate_),
      externalSecurity_(old.externalSecurity_),
      isExtraCall_(old.isExtraCall_),
      hasOutParams_(old.hasOutParams_),
      redefTime_(old.redefTime_),
      lastUsedTime_(old.lastUsedTime_),
      secKeySet_(h),
      isUniversal_(old.isUniversal_),
      executionMode_(old.getExecutionMode()),
      objectUID_(old.objectUID_),
      stateAreaSize_(old.stateAreaSize_),
      dllName_(old.dllName_, h),
      dllEntryPoint_(old.dllEntryPoint_, h),
      comRoutineParallelism_(old.comRoutineParallelism_),
      sasFormatWidth_(old.sasFormatWidth_, h),
      systemName_(old.systemName_, h),
      dataSource_(old.dataSource_, h),
      fileSuffix_(old.fileSuffix_, h),
      initialRowCost_(old.initialRowCost_),
      normalRowCost_(old.normalRowCost_),
      udfFanOut_(old.udfFanOut_),
      passThruDataNumEntries_(old.passThruDataNumEntries_),
      uecValues_(old.uecValues_, h),
      actionPosition_(old.actionPosition_),
      schemaVersionOfRoutine_(old.schemaVersionOfRoutine_),
      objectOwner_(0),
      schemaOwner_(0),
      privInfo_(NULL),
      privDescs_(NULL),
      heap_(h) {
  extRoutineName_ = new (h) ExtendedQualName(*old.extRoutineName_, h);
  extActionName_ = new (h) NAString(*old.extActionName_, h);
  intActionName_ = new (h) ComObjectName(*old.intActionName_, h);
  inParams_ = new (h) NAColumnArray(*old.inParams_, h);
  outParams_ = new (h) NAColumnArray(*old.outParams_, h);
  params_ = new (h) NAColumnArray(*old.params_, h);

  if (old.passThruDataNumEntries_ EQU 0) {
    passThruDataNumEntries_ = 0;
    passThruData_ = NULL;
    passThruDataSize_ = NULL;
  } else {
    passThruData_ = new (h) char *[(UInt32)passThruDataNumEntries_];
    passThruDataSize_ = new (h) long[(UInt32)passThruDataNumEntries_];
    for (int i = 0; i < passThruDataNumEntries_; i++) {
      passThruDataSize_[i] = old.passThruDataSize_[i];
      passThruData_[i] = new (h) char[(UInt32)passThruDataSize_[i] + 1];
      memcpy(passThruData_[i], old.passThruData_[i], (size_t)passThruDataSize_[i]);
      passThruData_[i][(size_t)passThruDataSize_[i]] = 0;  // make sure to Null terminate the string
    }
  }

  secKeySet_ = old.secKeySet_;

  heapSize_ = (h ? h->getTotalSize() : 0);
}

NARoutine::NARoutine(const QualifiedName &name, const TrafDesc *routine_desc, BindWA *bindWA, int &errorOccurred,
                     NAMemory *heap)
    : name_(name, heap),
      hashKey_(name, heap),
      language_(routine_desc->routineDesc()->language),
      UDRType_(routine_desc->routineDesc()->UDRType),
      sqlAccess_(routine_desc->routineDesc()->sqlAccess),
      transactionAttributes_(routine_desc->routineDesc()->transactionAttributes),
      maxResults_(routine_desc->routineDesc()->maxResults),
      stateAreaSize_(routine_desc->routineDesc()->stateAreaSize),
      externalFile_("", heap),
      externalPath_(routine_desc->routineDesc()->libraryFileName, heap),
      externalName_("", heap),
      librarySqlName_(routine_desc->routineDesc()->librarySqlName, COM_UNKNOWN_NAME, FALSE, heap)  // TODO
      ,
      libRedefTime_(routine_desc->routineDesc()->libRedefTime),
      libVersion_(routine_desc->routineDesc()->libVersion),
      libObjUID_(routine_desc->routineDesc()->libObjUID),
      signature_(routine_desc->routineDesc()->signature, heap),
      paramStyle_(routine_desc->routineDesc()->paramStyle),
      paramStyleVersion_(COM_ROUTINE_PARAM_STYLE_VERSION_1),
      isDeterministic_(routine_desc->routineDesc()->isDeterministic),
      isCallOnNull_(routine_desc->routineDesc()->isCallOnNull),
      isIsolate_(routine_desc->routineDesc()->isIsolate),
      externalSecurity_(routine_desc->routineDesc()->externalSecurity),
      isExtraCall_(FALSE)  // TODO
      ,
      hasOutParams_(FALSE),
      redefTime_(0)  // TODO
      ,
      lastUsedTime_(0),
      secKeySet_(heap),
      passThruDataNumEntries_(0),
      passThruData_(NULL),
      passThruDataSize_(0),
      udfFanOut_(1)  // TODO
      ,
      uecValues_(heap, 0),
      isUniversal_(0)  // TODO
      ,
      actionPosition_(0)  // TODO
      ,
      executionMode_(COM_ROUTINE_SAFE_EXECUTION),
      objectUID_(routine_desc->routineDesc()->objectUID),
      dllName_(routine_desc->routineDesc()->libraryFileName, heap),
      dllEntryPoint_(routine_desc->routineDesc()->externalName, heap),
      sasFormatWidth_("", heap)  // TODO
      ,
      systemName_("", heap)  // TODO
      ,
      dataSource_("", heap)  // TODO
      ,
      fileSuffix_("", heap)  // TODO
      ,
      schemaVersionOfRoutine_((COM_VERSION)0)  // TODO
      ,
      objectOwner_(routine_desc->routineDesc()->owner),
      schemaOwner_(routine_desc->routineDesc()->schemaOwner),
      privInfo_(NULL),
      privDescs_(NULL),
      heap_(heap) {
  char parallelism[5];
  CmGetComRoutineParallelismAsLit(routine_desc->routineDesc()->parallelism, parallelism);
  comRoutineParallelism_ = ((char *)parallelism);
  if (routine_desc->routineDesc()->libBlobHandle)
    libBlobHandle_ = NAString(routine_desc->routineDesc()->libBlobHandle, heap);
  else
    libBlobHandle_ = NAString();
  if (routine_desc->routineDesc()->libSchName)
    libSchName_ = NAString(routine_desc->routineDesc()->libSchName, heap);
  else
    libSchName_ = NAString();

  if (paramStyle_ == COM_STYLE_JAVA_CALL) {
    NAString extName(routine_desc->routineDesc()->externalName);
    size_t pos = extName.last('.');
    externalName_ = extName(pos + 1, (extName.length() - pos - 1));  // method_name
    externalFile_ = extName.remove(pos);                             // package_name.class_name
  } else {
    externalName_ = routine_desc->routineDesc()->externalName;
    if (language_ == COM_LANGUAGE_C || language_ == COM_LANGUAGE_CPP) {
      // Split the fully-qualified DLL name into a directory name and
      // simple file name
      ComUInt32 len = dllName_.length();
      if (len > 0) {
        size_t lastSlash = dllName_.last('/');
        if (lastSlash == NA_NPOS) {
          // No slash was found
          externalPath_ = ".";
          externalFile_ = dllName_;
        } else {
          // A slash was found. EXTERNAL PATH is everything before the
          // slash. EXTERNAL FILE is everything after.
          externalPath_ = dllName_;
          externalPath_.remove(lastSlash, len - lastSlash);
          externalFile_ = dllName_;
          externalFile_.remove(0, lastSlash + 1);
        }
      }
    }  // if (len > 0)
  }

  ComSInt32 colCount = routine_desc->routineDesc()->paramsCount;
  NAColumn *newCol = NULL;
  NAType *newColType = NULL;
  extRoutineName_ = new (heap) ExtendedQualName(name_, heap);
  extActionName_ = new (heap) NAString(heap);
  intActionName_ = new (heap) ComObjectName(heap);
  params_ = new (heap) NAColumnArray(heap);
  inParams_ = new (heap) NAColumnArray(heap);
  outParams_ = new (heap) NAColumnArray(heap);

  // to compute java signature
  ComFSDataType *paramType = new STMTHEAP ComFSDataType[colCount];
  ComUInt32 *subType = new STMTHEAP ComUInt32[colCount];
  ComColumnDirection *direction = new STMTHEAP ComColumnDirection[colCount];
  //
  // Construct the CostVectors
  // CQDs are checked at Bind time.
  int initCpuCost = -1;
  int initIOCost = -1;
  int initMsgCost = -1;

  CostScalar initialCpuCost(initCpuCost);
  CostScalar initialIOCost(initIOCost);
  CostScalar initialMsgCost(initMsgCost);

  initialRowCost_.setCPUTime(initCpuCost < 0 ? csMinusOne : initialCpuCost);
  initialRowCost_.setIOTime(initIOCost < 0 ? csMinusOne : initialIOCost);
  initialRowCost_.setMSGTime(initMsgCost < 0 ? csMinusOne : initialMsgCost);

  int normCpuCost = -1;
  int normIOCost = -1;
  int normMsgCost = -1;

  CostScalar normalCpuCost(normCpuCost);
  CostScalar normalIOCost(normIOCost);
  CostScalar normalMsgCost(normMsgCost);

  normalRowCost_.setCPUTime(normCpuCost < 0 ? csMinusOne : normalCpuCost);
  normalRowCost_.setIOTime(normIOCost < 0 ? csMinusOne : normalIOCost);
  normalRowCost_.setMSGTime(normMsgCost < 0 ? csMinusOne : normalMsgCost);

  TrafDesc *params_desc_list = routine_desc->routineDesc()->params;
  TrafColumnsDesc *param_desc;

  int i = 0;
  while (params_desc_list) {
    param_desc = params_desc_list->columnsDesc();

    // Create the new NAType.
    if (NAColumn::createNAType(param_desc->columnsDesc(), (const NATable *)NULL, 0, newColType, heap_)) {
      errorOccurred = TRUE;
      return;
    }
    if (param_desc->colname && strncmp(param_desc->colname, "#:", 2) == 0) {
      memset(param_desc->colname, 0, 2);
    }

    ComParamDirection colDirection = param_desc->paramDirection();

    // Create the new NAColumn and insert it into the NAColumnArray
    newCol = new (heap)
        NAColumn((const char *)UDRCopyString(param_desc->colname, heap), param_desc->colnumber, newColType, heap,
                 NULL  // NATable *
                 ,
                 USER_COLUMN, COM_NO_DEFAULT, NULL  // default value
                 ,
                 UDRCopyString("", heap)  // TODO:heading can it have some value
                 ,
                 param_desc->isUpshifted(), FALSE  // addedColumn
                 ,
                 (ComColumnDirection)colDirection, param_desc->isOptional(), (char *)COM_NORMAL_PARAM_TYPE_LIT);

    // We have to check for INOUT in both the
    // if's below
    if (COM_OUTPUT_PARAM == colDirection || COM_INOUT_PARAM == colDirection) {
      hasOutParams_ = TRUE;
      outParams_->insert(newCol);
      params_->insert(newCol);
    }

    if (COM_INPUT_PARAM == colDirection || COM_INOUT_PARAM == colDirection) {
      inParams_->insert(newCol);
      if (COM_INOUT_PARAM != colDirection) {
        params_->insert(newCol);
      }  // if not INOUT
    }    // if IN or INOUT

    // Gather the param attributes for LM from the paramDefArray previously
    // populated and from the routineparamList generated from paramDefArray.
    paramType[i] = (ComFSDataType)newColType->getFSDatatype();
    subType[i] = 0;  // default

    // Set subType for special cases detected by LM
    switch (paramType[i]) {
      case COM_SIGNED_BIN16_FSDT:
      case COM_SIGNED_BIN32_FSDT:
      case COM_SIGNED_BIN64_FSDT:
      case COM_UNSIGNED_BIN16_FSDT:
      case COM_UNSIGNED_BIN32_FSDT:
      case COM_UNSIGNED_BPINT_FSDT: {
        subType[i] = newColType->getPrecision();
        break;
      }

      case COM_DATETIME_FSDT: {
        switch (((DatetimeType *)newColType)->getSubtype()) {
          case DatetimeType::SUBTYPE_SQLDate:
            subType[i] = 1;
            break;
          case DatetimeType::SUBTYPE_SQLTime:
            subType[i] = 2;
            break;
          case DatetimeType::SUBTYPE_SQLTimestamp:
            subType[i] = 3;
            break;
        }
      }
    }  // end switch paramType[i]

    direction[i] = (ComColumnDirection)colDirection;

    params_desc_list = params_desc_list->next;
    i++;
  }  // for

  passThruDataNumEntries_ = 0;
  passThruData_ = NULL;
  passThruDataSize_ = NULL;

  if ((language_ == COM_LANGUAGE_JAVA) && (signature_.length() < 2)) {
    // Allocate buffer for generated signature
    char sigBuf[MAX_SIGNATURE_LENGTH];
    sigBuf[0] = '\0';

    // If the syntax specified a signature, pass that to LanguageManager.
    char *optionalSig = NULL;
    ComBoolean isJavaMain = ((str_cmp_ne(externalName_.data(), "main") == 0) ? TRUE : FALSE);

    LmResult createSigResult;
    LmJavaSignature *lmSignature = new (STMTHEAP) LmJavaSignature(NULL, STMTHEAP);
    createSigResult = lmSignature->createSig(paramType, subType, direction, colCount, COM_UNKNOWN_FSDT, 0, maxResults_,
                                             optionalSig, isJavaMain, sigBuf, MAX_SIGNATURE_LENGTH, CmpCommon::diags());
    NADELETE(lmSignature, LmJavaSignature, STMTHEAP);

    // Lm returned error. Lm fills diags area
    if (createSigResult != LM_ERR) signature_ = sigBuf;
  }

  getPrivileges(routine_desc->routineDesc()->priv_desc, bindWA);

  heapSize_ = (heap ? heap->getTotalSize() : 0);
}

NARoutine::~NARoutine() {
  // Call deepDelete() on NAColumnArray's.
  // The destructor does not do this.
  inParams_->deepDelete();
  outParams_->deepDelete();
  delete inParams_;
  delete outParams_;
  delete params_;  // Do not do a deepDelete() on params_ the
                   // elements are shared with in|outParams_.
  delete extRoutineName_;
  delete extActionName_;
  delete intActionName_;
  uecValues_.clear();  // delete all its elements.
  if (passThruData_ NEQ NULL) {
    for (int i = 0; i < passThruDataNumEntries_; i++)
      NADELETEBASIC(passThruData_[i], heap_);  // Can't use NADELETEARRAY on C types.
    NADELETEBASIC(passThruData_, heap_);       // Can't use NADELETEARRAY on C types.
  }


  if (privInfo_) NADELETE(privInfo_, PrivMgrUserPrivs, heap_);

  if (privDescs_) {
    NADELETEBASIC(privDescs_, heap_);
    privDescs_ = NULL;
  }
}

void NARoutine::setSasFormatWidth(NAString &width) { sasFormatWidth_ = width; }

void NARoutine::removePrivInfo(void) {
  if (privInfo_) {
    {
      NADELETE(privInfo_, PrivMgrUserPrivs, heap_);
      privInfo_ = NULL;
    }
  }
  secKeySet_.clear();
  secKeySet_ = NULL;
}

// ----------------------------------------------------------------------------
// method: getPrivileges
//
// If authorization is enabled, set privs based on the passed in priv_desc
// and set up query invalidation (security) keys for the routine.
// ----------------------------------------------------------------------------
void NARoutine::getPrivileges(TrafDesc *priv_desc, BindWA *bindWA) {
  if (!CmpCommon::context()->isAuthorizationEnabled() || ComUser::isRootUserID()) {
    privInfo_ = new (heap_) PrivMgrUserPrivs;
    privInfo_->setOwnerDefaultPrivs();
    return;
  }

  NAString privMDLoc = CmpSeabaseDDL::getSystemCatalogStatic();
  privMDLoc += ".\"";
  privMDLoc += SEABASE_PRIVMGR_SCHEMA;
  privMDLoc += "\"";
  PrivMgrCommands privInterface(privMDLoc.data(), CmpCommon::diags(), PrivMgr::PRIV_INITIALIZED);

  // get schema privileges
  NATable *schemaTable = NULL;
  CorrName cn(SEABASE_SCHEMA_OBJECTNAME, STMTHEAP, getRoutineName()->getQualifiedNameObj().getSchemaName(),
              getRoutineName()->getQualifiedNameObj().getCatalogName());
  cn.setSpecialType(ExtendedQualName::SCHEMA_TABLE);

  UInt32 parserFlags;
  SQL_EXEC_GetParserFlagsForExSqlComp_Internal(parserFlags);
  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);
  schemaTable = ActiveSchemaDB()->getNATableDB()->get(cn, bindWA, NULL, TRUE /* treat as write reference */);
  SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(parserFlags);

  if (schemaTable == NULL) {
    *CmpCommon::diags() << DgSqlCode(-1034);
    return;
  }

  if (priv_desc == NULL) {
    CmpSeabaseDDL cmpSBD(STMTHEAP);
    if (cmpSBD.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META)) {
      if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0) *CmpCommon::diags() << DgSqlCode(-4400);

      return;
    }

    ComObjectType objectType =
        (UDRType_ == COM_PROCEDURE_TYPE ? COM_STORED_PROCEDURE_OBJECT : COM_USER_DEFINED_ROUTINE_OBJECT);

    // get all privileges granted to routine object
    privDescs_ = new (heap_) PrivMgrDescList(heap_);  // initialize empty list
    PrivStatus privStatus = privInterface.getPrivileges(objectUID_, schemaOwner_, schemaTable->objectUid().get_value(),
                                                        schemaTable->getObjectType(), objectType, *privDescs_);
    cmpSBD.switchBackCompiler();

    if (privStatus == STATUS_ERROR) return;
  } else {
    // convert priv_desc (TrafPrivDesc) in privDescs_ member
    privDescs_ = new (heap_) PrivMgrDescList(heap_);  // initialize empty list
    TrafDesc *priv_grantees_desc = priv_desc->privDesc()->privGrantees;
    while (priv_grantees_desc) {
      PrivMgrDesc *privs = new (heap_) PrivMgrDesc(priv_grantees_desc->privGranteeDesc()->grantee);
      TrafDesc *objectPrivs = priv_grantees_desc->privGranteeDesc()->objectBitmap;

      PrivMgrCoreDesc objectDesc(objectPrivs->privBitmapDesc()->privBitmap,
                                 objectPrivs->privBitmapDesc()->privWGOBitmap);

      TrafDesc *priv_grantee_desc = priv_grantees_desc->privGranteeDesc();
      TrafDesc *columnPrivs = priv_grantee_desc->privGranteeDesc()->columnBitmaps;
      NAList<PrivMgrCoreDesc> columnDescs(NULL);
      while (columnPrivs) {
        PrivMgrCoreDesc columnDesc(columnPrivs->privBitmapDesc()->privBitmap,
                                   columnPrivs->privBitmapDesc()->privWGOBitmap,
                                   columnPrivs->privBitmapDesc()->columnOrdinal);
        columnDescs.insert(columnDesc);
        columnPrivs = columnPrivs->next;
      }

      privs->setTablePrivs(objectDesc);
      privs->setColumnPrivs(columnDescs);
      privs->setHasPublicPriv(ComUser::isPublicUserID(privs->getGrantee()));

      privDescs_->insert(privs);
      priv_grantees_desc = priv_grantees_desc->next;
    }
  }

  // get roles granted to current user
  NAList<int> roleIDs(heap_);
  NAList<int> grantees(heap_);
  if (ComUser::getCurrentUserRoles(roleIDs, grantees, CmpCommon::diags()) != 0) return;

  // set up privileges for current user
  privInfo_ = new (heap_) PrivMgrUserPrivs;
  privInfo_->initUserPrivs(roleIDs, privDescs_, ComUser::getCurrentUser(), objectUID_, &secKeySet_);

  // Add schema privileges and security keys
  if (schemaTable->getPrivInfo()) {
    privInfo_->setSchemaPrivBitmap(schemaTable->getPrivInfo()->getSchemaPrivBitmap());
    privInfo_->setSchemaGrantableBitmap(schemaTable->getPrivInfo()->getSchemaGrantableBitmap());

    if (privInfo_->getSchemaPrivBitmap().any()) {
      // Add schema's security keys
      for (int i = 0; i < schemaTable->getSecKeySet().entries(); i++) {
        ComSecurityKey key = schemaTable->getSecKeySet()[i];
        secKeySet_.insert(key);
      }
      privInfo_->setHasPublicPriv(schemaTable->getPrivInfo()->getHasPublicPriv());
    }
  }
}

int NARoutineDBKey::hash() const { return routine_.hash() ^ action_.hash(); }

int hashKey(const NARoutineDBKey &key) { return key.hash(); }
// NARoutineDB member functions (NARoutine caching)
#define DEFAULT_ROUTINEDB_CACHE_SZ 20  // in MB

// NARoutineDB public member functions.
// ======================================
// Constructor
NARoutineDB::NARoutineDB(NAMemory *h)
    : heap_(h),
      routinesToDeleteAfterStatement_(h),
      cacheMetaData_(TRUE),
      metadata(""),
      currentCacheSize_(0),
      refreshCacheInThisStatement_(FALSE),
      entries_(0),
      highWatermarkCache_(0),
      totalLookupsCount_(0),
      totalCacheHits_(0),
      NAKeyLookup<NARoutineDBKey, NARoutine>(NARoutineDB_INIT_SIZE, NAKeyLookupEnums::KEY_INSIDE_VALUE, h) {
  // Initial size.  Cannot use CQDs, since NARoutineDB is constructed as
  // part of SchemaDB object.
  defaultCacheSize_ = DEFAULT_ROUTINEDB_CACHE_SZ * 1024 * 1024;
  maxCacheSize_ = defaultCacheSize_;
}

NABoolean NARoutineDB::cachingMetaData() {
  maxCacheSize_ = CmpCommon::getDefaultLong(ROUTINE_CACHE_SIZE);
  return cacheMetaData_ && maxCacheSize_;
}

// ----------------------------------------------------------------------------
// method: moveRoutineToDeleteList
//
// This method removes the routine from the primary cache and adds it to the
// list of routines to delete after statement execution completes.
//
// cachedNARoutine is a pointer to an NARoutine entry
// key is a pointer to the routine key
// ----------------------------------------------------------------------------
void NARoutineDB::moveRoutineToDeleteList(NARoutine *cachedNARoutine, const NARoutineDBKey *key) {
  // routine is not cached, just return
  if (cachedNARoutine == NULL || !cachingMetaData()) return;

  // Add pointer to the routine to the list that will  be deleted after the
  // statement completes (to avoid affecting compile time)
  routinesToDeleteAfterStatement_.insert(cachedNARoutine);

  // The NARoutine in cache is dirty and needs to be removed.
  // Remove entry that the key is pointing to
  remove(key);

  // Adjust cache size
  if ((int)cachedNARoutine->getSize() <= currentCacheSize_) {
    currentCacheSize_ -= cachedNARoutine->getSize();
    if (entries_ > 0) entries_--;
  } else {
    currentCacheSize_ = 0;
    entries_ = 0;
  }
}

// Invalidate SPSQL routine caches
void NARoutineDB::invalidateSPSQLRoutines(LIST(NARoutine *) & routines) {
  if (routines.entries() == 0) {
    return;
  }
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  NAString sql("call ");
  sql.append(TRAFODION_SYSTEM_CATALOG);
  sql.append(".\"");
  sql.append(SEABASE_MD_SCHEMA);
  sql.append("\".");
  sql.append(SEABASE_SPSQL_EXECUTE_SPJ);
  sql.append("('INVALIDATE ");
  NABoolean found = FALSE;
  for (int i = 0; i < routines.entries(); i++) {
    NARoutine *routine = routines[i];
    if (routine->isSPSQL()) {
      if (found) {
        sql.append(",");
      } else {
        found = TRUE;
      }
      sql.append(routine->getSqlName().getQualifiedNameAsAnsiString().data());
    }
  }
  sql.append(";')");
  if (found) {
    unsigned int savedParserFlags = 0;
    SQL_EXEC_GetParserFlagsForExSqlComp_Internal(savedParserFlags);
    SQL_EXEC_SetParserFlagsForExSqlComp_Internal(INTERNAL_QUERY_FROM_EXEUTIL);
    PushAndSetSqlParserFlags parserFlags(INTERNAL_QUERY_FROM_EXEUTIL);
    // FIXME: should handle possible errors
    cliInterface.executeImmediate(sql);
    SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(savedParserFlags);
  }
}

// Cleanup NARoutine cache after the statement completes.  Remove entries
// using LRU policy, if the cache has grown too large.  The approach here is
// somewhat different from NATableDB caching, which deletes entries from the
// NATable cache if the statement was DDL that may have affected the table
// definition.   NATable caching also deletes tables from the cache at this
// time for performance reasons.
void NARoutineDB::resetAfterStatement() {
  // Delete 'dirty' NARoutine objects that were not deleted earlier
  // to save compile-time performance.
  if (routinesToDeleteAfterStatement_.entries()) {
    // Invalidate SPSQL routines cache
    invalidateSPSQLRoutines(routinesToDeleteAfterStatement_);
    // Clear the list of tables to delete after statement
    routinesToDeleteAfterStatement_.clear();
  }

  if (entries()) {
    // Reduce size of cache if it has grown too large.
    if (!enforceMemorySpaceConstraints()) cacheMetaData_ = FALSE;  // Disable cache if there is a problem.

    // Reset statement level flags
    refreshCacheInThisStatement_ = FALSE;

    // Clear 'accessed in current statement' flag for all cached NARoutines.
    NAHashDictionaryIterator<NARoutineDBKey, NARoutine> iter(*this);
    NARoutineDBKey *key;
    NARoutine *routine;
    iter.getNext(key, routine);
    while (key) {
      routine->getAccessedInCurStmt() = FALSE;
      iter.getNext(key, routine);
    }
  }
}

// ----------------------------------------------------------------------------
// method: free_entries_with_QI_key
//
// This method is sent a list of query invalidation keys.
// It looks through the list of routines to see if any of them need to be
// removed from cache because their definitions are no longer valid.
//
// numKeys - number of existing invalidation keys
// qiKeyArray - actual keys
// ----------------------------------------------------------------------------
void NARoutineDB::free_entries_with_QI_key(int numKeys, SQL_QIKEY *qiKeyArray) {
  NAHashDictionaryIterator<NARoutineDBKey, NARoutine> iter(*this);
  NARoutineDBKey *key;
  NARoutine *routine;
  iter.getNext(key, routine);
  while (key && routine) {
    // See if item should be removed
    if (qiCheckForInvalidObject(numKeys, qiKeyArray, routine->getRoutineID(), routine->getSecKeySet()))
      moveRoutineToDeleteList(routine, key);
    iter.getNext(key, routine);
  }
}

void NARoutineDB::reset_priv_entries() {
  int len = 500;
  char msg[len];

  // If error getting current roles, clear the cache list and return
  NAList<int> roleIDs(heap_);
  Int16 retcode = ComUser::getCurrentUserRoles(roleIDs, NULL /*don't update diags*/);

  // If unable to get roles, then just clear the list
  if (!ComUser::roleIDsCached()) {
    snprintf(msg, len,
             "NARoutineDB::reset_priv_entries, user: %s, retcode: %d,  "
             "unable to retrieve roles assigned to user, clearing cache",
             ComUser::getCurrentUsername(), retcode);
    QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", msg);
    setCachingOFF();
    setCachingON();
    return;
  }

  // If unable to get roles, then just clear the routine list
  if (retcode < 0 || roleIDs.entries() == 0) {
    setCachingOFF();
    setCachingON();
    return;
  }

  int userID = ComUser::getCurrentUser();

  NAHashDictionaryIterator<NARoutineDBKey, NARoutine> iter(*this);
  NARoutineDBKey *key;
  NARoutine *routine;
  iter.getNext(key, routine);
  while (key && routine) {
    routine->removePrivInfo();
    if (routine->getPrivDescs()) {
      PrivMgrUserPrivs *privInfo = new (routine->getHeap()) PrivMgrUserPrivs;
      ComSecurityKeySet secKeySet(routine->getHeap());
      bool result =
          privInfo->initUserPrivs(roleIDs, routine->getPrivDescs(), userID, routine->getRoutineID(), &secKeySet);
      if (result) {
        routine->setPrivInfo(privInfo);
        routine->setSecKeySet(secKeySet);
        std::string privDetails = routine->privInfo_->print();
        snprintf(msg, len, "NARoutineDB::reset_priv_entries, user: %s obj %s, %s", ComUser::getCurrentUsername(),
                 routine->getRoutineName()->getExtendedQualifiedNameAsString().data(), privDetails.c_str());
        QRLogger::log(CAT_SQL_EXE, LL_DEBUG, "%s", msg);
      }

      // result is false if unable to build the security key
      // assume no privileges, will be rechecked later.
      else {
        snprintf(msg, len, "DBRoutineDB::reset_priv_entries, user: %s obj %s, invalid security key",
                 ComUser::getCurrentUsername(), routine->getRoutineName()->getExtendedQualifiedNameAsString().data());
        QRLogger::log(CAT_SQL_EXE, LL_INFO, "%s", msg);

        routine->removePrivInfo();
      }
    }
    iter.getNext(key, routine);
  }
}

// This method follows the same semantics as NATableDB::removeNATable
void NARoutineDB::removeNARoutine(QualifiedName &routineName, ComQiScope qiScope, long objUID, NABoolean ddlXns,
                                  NABoolean atCommit) {
  NAHashDictionaryIterator<NARoutineDBKey, NARoutine> iter(*this);
  NARoutineDBKey *key = NULL;
  NARoutine *cachedNARoutine = NULL;

  NASet<long> objectUIDs(CmpCommon::statementHeap(), 1);

  // If there are no items in cache, skip
  if (entries() > 0) {
    NASet<long> objectUIDs(CmpCommon::statementHeap(), 1);

    // iterate over all entries and remove the ones that match the name
    iter.reset();
    iter.getNext(key, cachedNARoutine);

    while (key && cachedNARoutine) {
      if (cachedNARoutine->getSqlName() == routineName) {
        objectUIDs.insert(cachedNARoutine->getRoutineID());
        moveRoutineToDeleteList(cachedNARoutine, key);
      }
      iter.getNext(key, cachedNARoutine);
    }
  }

  // There are some scenarios where the affected object does not have an
  // NARoutine cache entry.  However, other processes may have this routine
  // in their caches.  Go ahead and create an invalidation key
  if (0 == objectUIDs.entries()) objectUIDs.insert(objUID);

  removeFromAllUsers(routineName, qiScope, COM_USER_DEFINED_ROUTINE_OBJECT, objectUIDs, ddlXns, atCommit);
}

// Find the NARoutine entry in the cache for 'key' or create it if not found.
// 1. Check for entry in cache.
// 2. If it doesn't exist, return.
// 3. If it does exist, check to see if this is first time entry accessed.
// 4. If so, check to see if entry is dirty (obsolete).
// 5. If dirty, remove key and add to list to remove entry after statement
//    completes.
// Note that BindWA is needed to be passed to this function so that we can
// use it with getRedefTime() is necessary.
NARoutine *NARoutineDB::get(BindWA *bindWA, const NARoutineDBKey *key) {
  // Check cache to see if a cached NARoutine object exists
  NARoutine *cachedNARoutine = NAKeyLookup<NARoutineDBKey, NARoutine>::get(key); /* 1 */

  totalLookupsCount_++;  // Statistics counter: number of lookups

  if (!cachedNARoutine || !cacheMetaData_) return NULL; /* 2 */

  totalCacheHits_++;  // Statistics counter: number of cache hits.

  cachedNARoutine->getAccessedInCurStmt() = TRUE;

  return cachedNARoutine;
}

// Insert NARoutine in NARoutineDB cache and update size.
void NARoutineDB::put(NARoutine *routine) {
  if (cacheMetaData_ && maxCacheSize_) {
    insert(routine);  // This function returns void.

    entries_++;
    currentCacheSize_ += routine->getSize();
    if (currentCacheSize_ > highWatermarkCache_) highWatermarkCache_ = currentCacheSize_;  // statistics counter
  }
}

// NARoutineDB private member functions.
// ======================================
// Check if cache size is within maximum allowed cache size.  If not,
// then remove entries in the cache based on the replacement policy,
// until the cache size drops below the allowed size.
// DO NOT CALL THIS ROUTINE IN THE MIDDLE OF A STATEMENT.
NABoolean NARoutineDB::enforceMemorySpaceConstraints() {
  NABoolean retval = TRUE;

  // Set cache size to CQD setting.
  maxCacheSize_ = CmpCommon::getDefaultLong(ROUTINE_CACHE_SIZE) * 1024 * 1024;

  // Check if cache size is within memory constraints
  if (currentCacheSize_ <= maxCacheSize_) return retval;

  // Need to reduce cache size

  // Loop through the list and attempt to remove the entries
  // that are least recently used (that is, that have the oldest
  // timestamps of when they were last used).  Do not allow the
  // number of times looped to exceed the number of entries in
  // cache.
  int loopCnt = 0, entries = this->entries();
  while (currentCacheSize_ > maxCacheSize_ && loopCnt++ < entries) {
    // Find least recently used NARoutine in cache.
    NAHashDictionaryIterator<NARoutineDBKey, NARoutine> iter(*this);
    NARoutineDBKey *key = 0, *oldestKey = 0;
    NARoutine *routine = 0, *oldestRoutine = 0;
    long oldestTime;

    iter.reset();
    iter.getNext(key, routine);
    if (key && routine)  // This should always be true.
    {
      oldestTime = routine->getLastUsedTime();
      while (key) {
        // Check routine for last used time.  Save oldest.  Must use
        // '<=' so that oldestKey will get set first time through loop.
        if (routine && routine->getLastUsedTime() <= oldestTime) {
          oldestTime = routine->getLastUsedTime();
          oldestKey = key;
          oldestRoutine = routine;
        }
        iter.getNext(key, routine);
      }
      remove(oldestKey);
      currentCacheSize_ -= oldestRoutine->getSize();
      if (entries_ > 0) entries_--;
      delete oldestRoutine;
    }
    if ((int)oldestRoutine->getSize() <= currentCacheSize_) {
      currentCacheSize_ -= oldestRoutine->getSize();
      if (entries_ > 0) entries_--;
    } else {
      currentCacheSize_ = 0;
      entries_ = 0;
    }
  }

  // return true indicating cache size is below maximum memory allowance.
  if (currentCacheSize_ > maxCacheSize_) retval = FALSE;
  return retval;
}

void NARoutineDB::flushCache() {
  // if something is cached
  if (cacheMetaData_) {
    // set the flag to indicate cache is clear
    cacheMetaData_ = FALSE;

    NAHashDictionaryIterator<NARoutineDBKey, NARoutine> iter(*this);
    NARoutineDBKey *key;
    NARoutine *routine;
    iter.getNext(key, routine);
    while (key && routine) {
      remove(key);
      delete routine;
      iter.getNext(key, routine);
    }

    routinesToDeleteAfterStatement_.clear();

    // set cache size to 0 to indicate nothing in cache
    currentCacheSize_ = 0;
    entries_ = 0;
    highWatermarkCache_ = 0;  // High watermark of currentCacheSize_
    totalLookupsCount_ = 0;   // reset NARoutine entries lookup counter
    totalCacheHits_ = 0;      // reset cache hit counter
    replacementCursor_ = 0;
  }
}

void NARoutineDB::getCacheStats(NARoutineCacheStats &stats) {
  stats.numLookups = totalLookupsCount_;
  stats.numCacheHits = totalCacheHits_;
  stats.currentCacheSize = currentCacheSize_;
  stats.highWaterMark = highWatermarkCache_;
  stats.maxCacheSize = maxCacheSize_;
  stats.numEntries = entries_;
}

//-----------------------------------------------------------------------
// NARoutineCacheStoredProcedure is a class that contains functions used by
// the NARoutineCache virtual table, whose purpose is to serve as an interface
// to the SQL/MX NARoutine cache statistics. This table is implemented as
// an internal stored procedure.
//-----------------------------------------------------------------------

SP_STATUS NARoutineCacheStatStoredProcedure::sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                                            SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                                            SP_ERROR_STRUCT *error) {
  if (numFields != 2) {
    // accepts 2 input columns
    error->error = arkcmpErrorISPWrongInputNum;
    strcpy(error->optionalString[0], "NARoutineCache");
    error->optionalInteger[0] = 2;
    return SP_FAIL;
  }

  // column as input parameter for ISP, specifiy cache of metadata or user context
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "instance char(16)  not null");
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "location char(16)  not null");
  return SP_SUCCESS;
}

SP_STATUS NARoutineCacheStatStoredProcedure::sp_NumOutputFields(int *numFields, SP_COMPILE_HANDLE spCompileObj,
                                                                SP_HANDLE spObj, SP_ERROR_STRUCT *error) {
  const int NUM_OF_OUTPUT = 6;

  *numFields = NUM_OF_OUTPUT;
  return SP_SUCCESS;
}

SP_STATUS NARoutineCacheStatStoredProcedure::sp_OutputFormat(SP_FIELDDESC_STRUCT *format, SP_KEYDESC_STRUCT keyFields[],
                                                             int *numKeyFields, SP_HANDLE spCompileObj, SP_HANDLE spObj,
                                                             SP_ERROR_STRUCT *error) {
  strcpy(&((format++)->COLUMN_DEF[0]), "Num_lookups      INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Num_cache_hits   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Num_entries      INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Current_cache_size   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "High_watermark   INT UNSIGNED");
  strcpy(&((format++)->COLUMN_DEF[0]), "Max_cache_size   INT UNSIGNED");

  return SP_SUCCESS;
}

SP_STATUS NARoutineCacheStatStoredProcedure::sp_ProcessRoutine(SP_PROCESS_ACTION action, SP_ROW_DATA inputData,
                                                               SP_EXTRACT_FUNCPTR eFunc, SP_ROW_DATA outputData,
                                                               SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                                               SP_KEYVALUE_FUNCPTR kFunc,
                                                               SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                                                               SP_ERROR_STRUCT *error) {
  if (action == SP_PROC_OPEN) {
    NARoutineCacheStatsISPIterator *it = new (GetCliGlobals()->exCollHeap()) NARoutineCacheStatsISPIterator(
        inputData, eFunc, error, GetCliGlobals()->currContext()->getCmpContextInfo(), GetCliGlobals()->exCollHeap());
    *spProcHandle = it;
    return SP_SUCCESS;
  }

  if (action == SP_PROC_FETCH) {
    NARoutineCacheStatsISPIterator *it = (NARoutineCacheStatsISPIterator *)(*spProcHandle);
    if (!it) {
      return SP_FAIL;
    }
    NARoutineCacheStats stats;
    if (!it->getNext(stats)) return SP_SUCCESS;
    fFunc(0, outputData, sizeof(int), &(stats.numLookups), 0);
    fFunc(1, outputData, sizeof(int), &(stats.numCacheHits), 0);
    fFunc(2, outputData, sizeof(int), &(stats.numEntries), 0);
    fFunc(3, outputData, sizeof(int), &(stats.currentCacheSize), 0);
    fFunc(4, outputData, sizeof(int), &(stats.highWaterMark), 0);
    fFunc(5, outputData, sizeof(int), &(stats.maxCacheSize), 0);
    return SP_MOREDATA;
  }

  if (action == SP_PROC_CLOSE) {
    if (*spProcHandle) NADELETEBASIC((NARoutineCacheStatsISPIterator *)(*spProcHandle), GetCliGlobals()->exCollHeap());
    return SP_SUCCESS;
  }
  return SP_SUCCESS;
}

SP_STATUS NARoutineCacheStatStoredProcedure::sp_ProcessAction(SP_PROCESS_ACTION action, SP_ROW_DATA inputData,
                                                              SP_EXTRACT_FUNCPTR eFunc, SP_ROW_DATA outputData,
                                                              SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                                              SP_KEYVALUE_FUNCPTR kFunc,
                                                              SP_PROCESS_HANDLE *spProcHandle, SP_HANDLE spObj,
                                                              SP_ERROR_STRUCT *error) {
  struct InfoStruct {
    int counter;
  };

  SP_STATUS status = SP_SUCCESS;
  InfoStruct *is = NULL;

  NARoutineDB *tableDB = ActiveSchemaDB()->getNARoutineActionDB();

  switch (action) {
    case SP_PROC_OPEN:
      is = new InfoStruct;
      is->counter = 0;
      *spProcHandle = is;
      break;

    case SP_PROC_FETCH:
      is = (InfoStruct *)(*spProcHandle);
      if (is == NULL) {
        status = SP_FAIL;
        break;
      }

      if (is->counter > 0) break;
      is->counter++;
      fFunc(0, outputData, sizeof(int), &(tableDB->totalLookupsCount_), 0);
      fFunc(1, outputData, sizeof(int), &(tableDB->totalCacheHits_), 0);
      fFunc(2, outputData, sizeof(int), &(tableDB->entries_), 0);
      fFunc(3, outputData, sizeof(int), &(tableDB->currentCacheSize_), 0);
      fFunc(4, outputData, sizeof(int), &(tableDB->highWatermarkCache_), 0);
      fFunc(5, outputData, sizeof(int), &(tableDB->maxCacheSize_), 0);
      status = SP_MOREDATA;
      break;

    case SP_PROC_CLOSE:
      delete (InfoStruct *)(*spProcHandle);
      break;
  }
  return status;
}

void NARoutineCacheStatStoredProcedure::Initialize(SP_REGISTER_FUNCPTR regFunc) {
  regFunc("NAROUTINECACHE", sp_Compile, sp_InputFormat, 0, sp_NumOutputFields, sp_OutputFormat, sp_ProcessRoutine, 0,
          CMPISPVERSION);
  regFunc("NAROUTINEACTIONCACHE", sp_Compile, sp_InputFormat, 0, sp_NumOutputFields, sp_OutputFormat, sp_ProcessAction,
          0, CMPISPVERSION);
}

//-----------------------------------------------------------------------
// NARoutineCacheDeleteStoredProcedure is a class that contains functions used
// to delete the contents of the  NARoutineCache virtual table. The delete
// function is implemented as an internal stored procedure.
//-----------------------------------------------------------------------

SP_STATUS NARoutineCacheDeleteStoredProcedure::sp_InputFormat(SP_FIELDDESC_STRUCT *inputFieldFormat, int numFields,
                                                              SP_COMPILE_HANDLE spCompileObj, SP_HANDLE spObj,
                                                              SP_ERROR_STRUCT *error)

{
  if (numFields != 1) {
    // The delete NAROUTINE built-in function accepts one input columns
    error->error = arkcmpErrorISPWrongInputNum;
    strcpy(error->optionalString[0], "NARoutineCache");
    error->optionalInteger[0] = 1;
    return SP_FAIL;
  }

  // Describe input columns
  strcpy(&((inputFieldFormat++)->COLUMN_DEF[0]), "cachetype char(10) not null");
  return SP_SUCCESS;
}

SP_STATUS NARoutineCacheDeleteStoredProcedure::sp_Process(SP_PROCESS_ACTION action, SP_ROW_DATA inputData,
                                                          SP_EXTRACT_FUNCPTR eFunc, SP_ROW_DATA outputData,
                                                          SP_FORMAT_FUNCPTR fFunc, SP_KEY_VALUE keys,
                                                          SP_KEYVALUE_FUNCPTR kFunc, SP_PROCESS_HANDLE *spProcHandle,
                                                          SP_HANDLE spObj, SP_ERROR_STRUCT *error) {
  // Not sure what "counter" is used for, can be removed?
  struct InfoStruct {
    int counter;
    int resetOnly;
  };

  SP_STATUS status = SP_SUCCESS;
  InfoStruct *is = NULL;
  NARoutineDB *routineDB = NULL;
  NARoutineDB *actionDB = NULL;

  switch (action) {
    case SP_PROC_OPEN: {
      is = new InfoStruct;
      is->counter = 0;

      // See if reset only specified
      char buf[10];
      if (eFunc(0, inputData, strlen(buf), buf, FALSE) == SP_ERROR_EXTRACT_DATA) {
        error->error = arkcmpErrorISPFieldDef;
        return SP_FAIL;
      }
      buf[5] = '\0';
      NAString cacheType(buf);
      cacheType.toLower();
      is->resetOnly = (cacheType == "reset") ? 1 : 0;

      *spProcHandle = is;
      break;
    }

    case SP_PROC_FETCH: {
      is = (InfoStruct *)(*spProcHandle);
      if (is == NULL) {
        status = SP_FAIL;
        break;
      }

      // clear out NARoutineCache
      routineDB = ActiveSchemaDB()->getNARoutineDB();
      if (is->resetOnly)
        routineDB->reset_priv_entries();
      else {
        routineDB->setCachingOFF();
        routineDB->setCachingON();
      }

      // clear out action caching
      actionDB = ActiveSchemaDB()->getNARoutineActionDB();
      if (actionDB) {
        actionDB->setCachingOFF();
        actionDB->setCachingON();
      }
      break;
    }

    case SP_PROC_CLOSE:
      delete (InfoStruct *)(*spProcHandle);
      break;
  }
  return status;
}

void NARoutineCacheDeleteStoredProcedure::Initialize(SP_REGISTER_FUNCPTR regFunc) {
  regFunc("NAROUTINECACHEDELETE", sp_Compile, sp_InputFormat, 0, sp_NumOutputFields, sp_OutputFormat, sp_Process, 0,
          CMPISPVERSION);
}

NARoutineCacheStatsISPIterator::NARoutineCacheStatsISPIterator(SP_ROW_DATA inputData, SP_EXTRACT_FUNCPTR eFunc,
                                                               SP_ERROR_STRUCT *error,
                                                               const NAArray<CmpContextInfo *> &ctxs, CollHeap *h)
    : ISPIterator(ctxs, h) {
  initializeISPCaches(inputData, eFunc, error, ctxs, contextName_, currCacheIndex_);
}

NABoolean NARoutineCacheStatsISPIterator::getNext(NARoutineCacheStats &stats) {
  // Only for remote tdm_arkcmp with 0 context
  if (currCacheIndex_ == -1) {
    ActiveSchemaDB()->getNARoutineDB()->getCacheStats(stats);
    currCacheIndex_ = -2;
    return TRUE;
  }

  // fetch QueryCaches of all CmpContexts with name equal to contextName_
  if (currCacheIndex_ > -1 && currCacheIndex_ < ctxInfos_.entries()) {
    if (!ctxInfos_[currCacheIndex_]->isSameClass(
            contextName_.data())                                 // current context name is not equal to contextName_
        && contextName_.compareTo("ALL", NAString::exact) != 0)  // and contextName_ is not "ALL"
    {                                                            // go to next context in ctxInfos_
      currCacheIndex_++;
      return getNext(stats);
    }
    ctxInfos_[currCacheIndex_++]->getCmpContext()->getSchemaDB()->getNARoutineDB()->getCacheStats(stats);
    return TRUE;
  }
  // all entries of all caches are fetched, we are done!
  return FALSE;
}
