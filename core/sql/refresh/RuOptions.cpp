
/* -*-C++-*-
******************************************************************************
*
* File:         RuOptions.cpp
* Description:  API to the command-line parameters of the REFRESH utility.
*
*
* Created:      01/09/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuOptions.h"
#include "uofsIpcMessageTranslator.h"
#include "RuException.h"

// the refresh messages output filename will be initialialized to empty string
CDSString CRUOptions::defOutFilename = "REFRESH.LOG";

//--------------------------------------------------------------------------//
//	Constructors
//--------------------------------------------------------------------------//

CRUOptions::CRUOptions()
    : invocType_(SINGLE_MV),
      isRecompute_(FALSE),
      isCancel_(FALSE),
      lcType_(DONTCARE_LC),
      catalogName_(""),
      schemaName_(""),
      objectName_(""),
      fullName_(""),
      defaultSchema_(""),
#if defined(NA_WINNT)
      outFilename_(defOutFilename),
#else
      outFilename_(""),
#endif
      forceFilename_("") {
}

CRUOptions::CRUOptions(const CRUOptions &other)
    : invocType_(other.invocType_),
      isRecompute_(other.isRecompute_),
      isCancel_(other.isCancel_),
      lcType_(other.lcType_),
      catalogName_(other.catalogName_),
      schemaName_(other.schemaName_),
      objectName_(other.objectName_),
      fullName_(other.fullName_),
      outFilename_(other.outFilename_),
      forceFilename_(other.forceFilename_) {}

//--------------------------------------------------------------------------//
//	CRUOptions::FindDebugOption()
//
//	Search for the option using the <testpoint, objectName> pair.
//	If the object name in the options list is empty,
//	the search does not use it as a criterion.
//
//--------------------------------------------------------------------------//

CRUOptions::DebugOption *CRUOptions::FindDebugOption(int testpoint, const CDSString &objName) {
  DSListPosition pos = debugOptionList_.GetHeadPosition();
  while (NULL != pos) {
    DebugOption &opt = debugOptionList_.GetNext(pos);

    if (testpoint != opt.testpoint_) {
      continue;
    }

    if (0 == opt.objName_.GetLength() || objName == opt.objName_) {
      return &opt;
    }
  }

  return NULL;
}

//--------------------------------------------------------------------------//
//	CRUOptions::AddDebugOption()
//--------------------------------------------------------------------------//

void CRUOptions::AddDebugOption(int testpoint, const CDSString &objName) {
  CRUOptions::DebugOption opt;

  opt.testpoint_ = testpoint;
  opt.objName_ = objName;

  debugOptionList_.AddTail(opt);

  // when debuuging is set outfile is always set to default out filename
  if (outFilename_.IsEmpty()) outFilename_ = defOutFilename;
}

#ifdef _DEBUG
//--------------------------------------------------------------------------//
//	CRUOptions::Dump()
//--------------------------------------------------------------------------//
void CRUOptions::Dump(CDSString &to) {
  to += "\n\t\tCOMMAND OPTIONS DUMP\n\n";

  to += "Catalog name = " + GetCatalogName() + "\n";
  to += "Schema name = " + GetSchemaName() + "\n";
  to += "Object name = " + GetObjectName() + " ";

  switch (GetInvocType()) {
    case SINGLE_MV:
      to += "(MV)\n";
      break;

    case CASCADE:
      to += "(CASCADE)\n";
      break;

    case MV_GROUP:
      to += "(MV GROUP)\n";
      break;

    default:
      RUASSERT(FALSE);
  }

  to += "Recompute mode = ";
  to += (IsRecompute() ? "YES" : "NO");
  to += "\n";

  to += "DDL locks cancel mode = ";
  to += (IsCancel() ? "YES" : "NO");
  to += "\n";

  switch (GetLogCleanupType()) {
    case DONTCARE_LC:
      to += "LOG CLEANUP NOT SPECIFIED";
      break;

    case WITH_LC:
      to += "INCLUDING LOG CLEANUP";
      break;

    case WITHOUT_LC:
      to += "NOT INCLUDING LOG CLEANUP";
      break;

    case DO_ONLY_LC:
      to += "LOG CLEANUP ONLY";
      break;

    default:
      ASSERT(FALSE);
  }

  to += "\nOutput file = \"";
  to += GetOutputFilename();
  to += "\"\n";

  char buf[200];
  DSListPosition pos = debugOptionList_.GetHeadPosition();
  while (NULL != pos) {
    DebugOption &opt = debugOptionList_.GetNext(pos);
    sprintf(buf, "TESTPOINT %6d\t%s\n", opt.testpoint_, opt.objName_.c_string());
    to += CDSString(buf);
  }
}
#endif

//--------------------------------------------------------------------------//
//	CRUOptions::DebugOption &operator = ()
//--------------------------------------------------------------------------//

CRUOptions::DebugOption &CRUOptions::DebugOption::operator=(const CRUOptions::DebugOption &other) {
  testpoint_ = other.testpoint_;
  objName_ = other.objName_;

  return *this;
}

//--------------------------------------------------------------------------//
//	CRUOptions::StoreData()
//
//	The LoadData/StoreData methods move only the output
//	file's name and the debug options between the processes.
//--------------------------------------------------------------------------//
void CRUOptions::StoreData(CUOFsIpcMessageTranslator &translator) {
  int stringSize;

  // Output filename
  stringSize = outFilename_.GetLength() + 1;
  translator.WriteBlock(&stringSize, sizeof(int));
  translator.WriteBlock(outFilename_.c_string(), stringSize);

  // Force filename
  stringSize = forceFilename_.GetLength() + 1;
  translator.WriteBlock(&stringSize, sizeof(int));
  translator.WriteBlock(forceFilename_.c_string(), stringSize);

  // Debug options
  int size = debugOptionList_.GetCount();
  translator.WriteBlock(&size, sizeof(int));

  DSListPosition pos = debugOptionList_.GetHeadPosition();
  while (NULL != pos) {
    DebugOption &opt = debugOptionList_.GetNext(pos);

    translator.WriteBlock(&(opt.testpoint_), sizeof(int));

    stringSize = opt.objName_.GetLength() + 1;
    translator.WriteBlock(&stringSize, sizeof(int));
    translator.WriteBlock(opt.objName_.c_string(), stringSize);
  }
}

//--------------------------------------------------------------------------//
//	CRUOptions::LoadData()
//--------------------------------------------------------------------------//
void CRUOptions::LoadData(CUOFsIpcMessageTranslator &translator) {
  char buf[PACK_BUFFER_SIZE];
  int stringSize;

  // Output filename
  translator.ReadBlock(&stringSize, sizeof(int));
  translator.ReadBlock(buf, stringSize);

  CDSString outFileName(buf);
  SetOutputFilename(outFileName);

  // Force filename
  translator.ReadBlock(&stringSize, sizeof(int));
  translator.ReadBlock(buf, stringSize);

  CDSString forceFileName(buf);
  SetForceFilename(forceFileName);

  // Debug options
  int size;
  translator.ReadBlock(&size, sizeof(int));

  for (int i = 0; i < size; i++) {
    int testpoint;

    translator.ReadBlock(&testpoint, sizeof(int));

    translator.ReadBlock(&stringSize, sizeof(int));

    RUASSERT(PACK_BUFFER_SIZE > stringSize);

    translator.ReadBlock(buf, stringSize);

    CDSString objName(buf);
    AddDebugOption(testpoint, objName);
  }
}
