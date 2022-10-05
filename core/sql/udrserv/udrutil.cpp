
/* -*-C++-*-
*****************************************************************************
*
* File:         udrutil.cpp
* Description:  Generic utilities...
* Created:      01/01/2001
* Language:     C++
*
*****************************************************************************
*/
#include "common/Platform.h"

#include "executor/sql_buffer.h"
#include "common/Int64.h"
#include "udrutil.h"
#include "udrdefs.h"
#include "udrglobals.h"
#include "LmLangManager.h"
#include "LmParameter.h"
#include "spinfo.h"
#include "export/ComDiags.h"
#include <stdarg.h>
#include <ctype.h>
#include "common/ComRtUtils.h"

extern char *getDatatypeAsString(int datatype, NABoolean extFormat = FALSE);

FILE *UdrTraceFile = stdout;

#define TF_STRING(x) ((x) ? ("TRUE") : ("FALSE"))

void displaySqlBuffer(SqlBuffer *sbuf, int sbuflen, ostream &os) {
  os << "Display an SQL Buffer:" << endl;
  os << "  Buffer Size          : " << sbuf->get_buffer_size() << endl;
  os << "  Used Size            : " << sbuf->get_used_size() << endl;
  os << "  Free Size            : " << (sbuf->get_buffer_size() - sbuf->get_used_size()) << endl;

  switch (sbuf->bufType()) {
    case SqlBufferBase::NORMAL_:
      os << "  Buffer Type          : Normal" << endl;
      break;
    case SqlBufferBase::DENSE_:
      os << "  Buffer Type          : Dense" << endl;
      break;
    case SqlBufferBase::OLT_:
      os << "  Buffer Type          : OLT" << endl;
      break;
    default:
      os << "  Buffer Type          : Unknown (" << sbuf->bufType() << ")" << endl;
      break;
  }

  os << "  Packed?              : " << TF_STRING(sbuf->packed()) << endl;
  os << "  Empty?               : " << TF_STRING(sbuf->isFull()) << endl;
  os << "  Full?                : " << TF_STRING(sbuf->isEmpty()) << endl;
  os << "  Free?                : " << TF_STRING(sbuf->isFree()) << endl;

  int numtuppdescs = sbuf->getTotalTuppDescs();
  os << "  Total Tupp Descs     : " << numtuppdescs << endl;
  os << "  Processed Tupp Descs : " << sbuf->getProcessedTuppDescs() << endl;

#ifdef SQL_BUFFER_SIGNATURE
  char csig[100];
  convertInt64ToAscii(sbuf->getSignature(), csig);
  os << "  Signature        : " << csig << endl;
#endif

  tupp_descriptor *td;
  char *ctupp;
  ControlInfo *ci;

  for (int i = 1; i <= numtuppdescs; i++) {
    td = sbuf->getTuppDescriptor(i);
    if (td == NULL) {
      break;
    }

    ctupp = td->getTupleAddress();

    if (td->isDataTuple()) {
      ServerDebug("");
      ServerDebug("Data Tuple:");
      os << "    Tupp(" << i << ") Ref Count     : " << td->getReferenceCount() << endl;
      os << "    Tupp(" << i << ") Allocated Size: " << td->getAllocatedSize() << endl;
      dumpBuffer((unsigned char *)ctupp, td->getAllocatedSize());
    }

    else if (td->isStatsTuple()) {
      ServerDebug("");
      ServerDebug("Stats Tuple:");
      os << "    Tupp(" << i << ") Ref Count     : " << td->getReferenceCount() << endl;
      os << "    Tupp(" << i << ") Allocated Size: " << td->getAllocatedSize() << endl;
    }

    else if (td->isDiagsTuple()) {
      ServerDebug("");
      ServerDebug("Diags Tuple:");
      os << "    Tupp(" << i << ") Ref Count     : " << td->getReferenceCount() << endl;
      os << "    Tupp(" << i << ") Allocated Size: " << td->getAllocatedSize() << endl;
    }

    else if (td->isControlTuple()) {
      ServerDebug("");
      ServerDebug("Control Tuple:");
      os << "    Tupp(" << i << ") Ref Count     : " << td->getReferenceCount() << endl;
      os << "    Tupp(" << i << ") Allocated Size: " << td->getAllocatedSize() << endl;

      ci = (ControlInfo *)td->getTupleAddress();
      up_state us = ci->getUpState();
      down_state ds = ci->getDownState();

      os << "    Tupp(" << i << ")   UpState         : " << endl;
      os << "    Tupp(" << i << ")     Parent Index  : " << us.parentIndex << endl;
      os << "    Tupp(" << i << ")     Down Index    : " << us.downIndex << endl;

      switch (us.status) {
        case ex_queue::Q_NO_DATA: {
          os << "    Tupp(" << i << ")     Status        : Q_NO_DATA" << endl;
          break;
        }
        case ex_queue::Q_OK_MMORE: {
          os << "    Tupp(" << i << ")     Status        : Q_OK_MMORE" << endl;
          break;
        }
        case ex_queue::Q_SQLERROR: {
          os << "    Tupp(" << i << ")     Status        : Q_SQLERROR" << endl;
          break;
        }
        case ex_queue::Q_INVALID: {
          os << "    Tupp(" << i << ")     Status        : Q_INVALID" << endl;
          break;
        }
        case ex_queue::Q_GET_DONE: {
          os << "    Tupp(" << i << ")     Status        : Q_GET_DONE" << endl;
          break;
        }
        default: {
          os << "    Tupp(" << i << ")     Request       : Unknown (" << us.status << ")" << endl;
          break;
        }
      }

      os << "    Tupp(" << i << ")     Match Nbr     : " << us.getMatchNo() << endl;
      os << "    Tupp(" << i << ")   DownState       : " << endl;

      switch (ds.request) {
        case ex_queue::GET_N: {
          os << "    Tupp(" << i << ")     Request       : GET_N" << endl;
          break;
        }
        case ex_queue::GET_ALL: {
          os << "    Tupp(" << i << ")     Request       : GET_ALL" << endl;
          break;
        }
        case ex_queue::GET_NOMORE: {
          os << "    Tupp(" << i << ")     Request       : GET_NOMORE" << endl;
          break;
        }
        case ex_queue::GET_EMPTY: {
          os << "    Tupp(" << i << ")     Request       : GET_EMPTY" << endl;
          break;
        }
        case ex_queue::GET_EOD: {
          os << "    Tupp(" << i << ")     Request       : GET_EOD" << endl;
          break;
        }
        case ex_queue::GET_NEXT_N: {
          os << "    Tupp(" << i << ")     Request       : GET_NEXT_N" << endl;
          break;
        }
        case ex_queue::GET_NEXT_N_MAYBE_WAIT: {
          os << "    Tupp(" << i << ")     Request       : GET_NEXT_N_MAYBE_WAIT" << endl;
          break;
        }
        case ex_queue::GET_NEXT_0_MAYBE_WAIT: {
          os << "    Tupp(" << i << ")     Request       : GET_NEXT_0_MAYBE_WAIT" << endl;
          break;
        }
        default: {
          os << "    Tupp(" << i << ")     Request       : Unknown (" << ds.request << ")" << endl;
          break;
        }
      }

      os << "    Tupp(" << i << ")     Request       : " << ds.request << endl;
      os << "    Tupp(" << i << ")     Request Value : " << ds.requestValue << endl;
      os << "    Tupp(" << i << ")     Nbr Get Nexts : " << ds.numGetNextsIssued << endl;
      os << "    Tupp(" << i << ")     Parent Index  : " << ds.parentIndex << endl;

      os << "    Tupp(" << i << ")   Buff Seq Nbr    : " << ci->getBufferSequenceNumber() << endl;

      os << "    Tupp(" << i << ")   Diags Area?     : " << TF_STRING(ci->getIsDiagsAreaPresent()) << endl;
      os << "    Tupp(" << i << ")   Ext Diags Area? : " << TF_STRING(ci->getIsExtDiagsAreaPresent()) << endl;
      os << "    Tupp(" << i << ")   Data Row?       : " << TF_STRING(ci->getIsDataRowPresent()) << endl;
      os << "    Tupp(" << i << ")   Stats Area?     : " << TF_STRING(ci->getIsStatsAreaPresent()) << endl;
      os << "    Tupp(" << i << ")   ExtStats Area?  : " << TF_STRING(ci->getIsExtStatsAreaPresent()) << endl;

    }  // if (td->isControlTuple())

    else {
      // some other type of tuple...
      ServerDebug("");
      ServerDebug("Unknown Tuple:");
      os << "    Tupp(" << i << ") Ref Count     : " << td->getReferenceCount() << endl;
      os << "    Tupp(" << i << ") Allocated Size: " << td->getAllocatedSize() << endl;
    }

  }  // for (long i = 1; i <= numtuppdescs; i++)

  os << endl;

}  // displaySqlBuffer

// This method is not invoked from anywhere, need to determine if we should keep it, if so, we have to add
// a way to invoke it.
void displayStatement(SQLSTMT_ID s) {
  cout << "  Statement ID           :" << endl;
  cout << "    Version              :" << s.version << endl;
  cout << "    Name Mode            :" << s.name_mode << endl;
  cout << "    Module               :" << endl;
  cout << "      Version            :" << s.module->version << endl;
  cout << "      Name               :" << s.module->module_name << endl;

#if defined(_MSC_VER)
  char ct[100];
  convertInt64ToAscii(s.module->creation_timestamp, ct);
  cout << "      Creation Timestamp :" << ct << endl;
#endif

  cout << "      Char Set           :" << s.module->charset << endl;
  cout << "      Name Length        :" << s.module->module_name_len << endl;
  cout << "    Identifier           :" << s.identifier << endl;
  cout << "    Handle               :" << s.handle << endl;
  cout << "    Char Set             :" << s.charset << endl;
  cout << "    Identifier Length    :" << s.identifier_len << endl;
}  // displayStatement

void dumpLmParameter(LmParameter &p, int i, const char *prefix) {
  if (prefix == NULL) prefix = "";

  ServerDebug("%s [%ld] Name [%s]", prefix, i, p.getParamName() ? p.getParamName() : "");
  ServerDebug("%s     FS type %s, prec %d, scale %d, charset %d", prefix, getDatatypeAsString(p.fsType()),
              (int)p.prec(), (int)p.scale(), (int)p.encodingCharSet());
  ServerDebug("%s     direction %s, collation %d, objMap %ld, resultSet %s", prefix, getDirectionName(p.direction()),
              (int)p.collation(), (int)p.objMapping(), TF_STRING(p.resultSet()));
  ServerDebug("%s     IN  offset %d, len %d, null offset %d, null len %d", prefix, (int)p.inDataOffset(),
              (int)p.inSize(), (int)p.inNullIndOffset(), (int)p.inNullIndSize());
  ServerDebug("%s         vclen offset %d, vclen len %d", prefix, (int)p.inVCLenIndOffset(),
              (int)p.inVCLenIndSize());
  ServerDebug("%s     OUT offset %d, len %d, null offset %d, null len %d", prefix, (int)p.outDataOffset(),
              (int)p.outSize(), (int)p.outNullIndOffset(), (int)p.outNullIndSize());
  ServerDebug("%s         vclen offset %d, vclen len %d", prefix, (int)p.outVCLenIndOffset(),
              (int)p.outVCLenIndSize());

}  // dumpLmParameter

void dumpBuffer(unsigned char *buffer) { dumpBuffer(buffer, str_len((char *)buffer)); }

void dumpBuffer(unsigned char *buffer, size_t len) {
  if (buffer == NULL) {
    return;
  }

  //
  // We will print the buffer 16 characters at a time. For each group
  // of 16 characters will we build two strings. One we call the "hex"
  // string and the other the "ascii" string. They will be formatted
  // as follows:
  //
  //  hex:   "<hex pair> <hex pair> ..."
  //  ascii: "<ascii value><ascii value>..."
  //
  // where each <hex pair> is a 4-character hex representation of
  // 2-bytes and each <ascii value> is 1 character.
  //
  const int CHARS_PER_LINE = 16;
  const int ASCII_BUF_LEN = CHARS_PER_LINE + 1;

  const int HEX_BUF_LEN = (2 * CHARS_PER_LINE)    // 2 hex characters per byte of input
                            + (CHARS_PER_LINE / 2)  // one space between each <hex pair>
                            + 1;                    // null terminator

  //
  // The 100-byte pads in the following two buffers are only used as
  // safeguards to prevent corrupting the stack. The code should work
  // without the padding.
  //
  char hexBuf[HEX_BUF_LEN + 100];
  char asciiBuf[ASCII_BUF_LEN + 100];
  size_t i, j, hexOffset, asciiOffset;
  size_t startingOffset;
  int nCharsWritten;

  //
  // This message will be used for buffer overflow assertion failures
  //
  const char *msg = "Buffer overflow in dumpBuffer() routine";

  i = 0;
  while (i < len) {
    //
    // Initialize the two buffers with blank padding and null
    // terminators
    //
    memset(hexBuf, ' ', HEX_BUF_LEN);
    memset(asciiBuf, ' ', ASCII_BUF_LEN);
    hexBuf[HEX_BUF_LEN - 1] = '\0';
    asciiBuf[ASCII_BUF_LEN - 1] = '\0';
    hexOffset = 0;
    asciiOffset = 0;
    startingOffset = i;

    //
    // Inside the following for loop hexOffset should be incremented
    // by 2 or 3 and asciiOffset incremented by 1.
    //
    for (j = 0; j < CHARS_PER_LINE && i < len; j++, i++) {
      //
      // Write a 2-character hex value to the hex buffer. The %X
      // format expects an int value.
      //
      nCharsWritten = sprintf(&hexBuf[hexOffset], "%02X", (int)buffer[i]);
      UDR_ASSERT(nCharsWritten == 2, msg);
      hexOffset += 2;

      //
      // Add a space to the hex buffer following each pair of bytes
      //
      if (j % 2 == 1) {
        hexBuf[hexOffset++] = ' ';
      }

      //
      // Write one character to the ascii buffer
      //
      char c;
      if (!isprint(buffer[i])) {
        c = '.';
      } else {
        c = buffer[i];
      }
      asciiBuf[asciiOffset++] = c;

    }  // for (j = 0; j < CHARSPERLINE && i < len; j++, i++)

    UDR_ASSERT(hexOffset < HEX_BUF_LEN, msg);
    UDR_ASSERT(asciiOffset < ASCII_BUF_LEN, msg);
    UDR_ASSERT(hexBuf[HEX_BUF_LEN - 1] == '\0', msg);
    UDR_ASSERT(asciiBuf[ASCII_BUF_LEN - 1] == '\0', msg);

    ServerDebug("%08X  %-*s |  %s", (int)startingOffset, (int)(HEX_BUF_LEN - 1), hexBuf, asciiBuf);

  }  // while (i < len)

  ServerDebug("");

}  // dumpBuffer

void dumpComCondition(ComCondition *cc, char *ind) {
  // ServerDebug("%s  Class Origin        : %s",  ind, cc->getClassOrigin());
  // ServerDebug("%s  Sub Class Origin    : %s",  ind, cc->getSubClassOrigin());
  // ServerDebug("%s  Message             : %s",  ind, cc->getMessageText());
  // ServerDebug("%s  Message Length      : %ld", ind,
  //            (long) cc->getMessageLength());

  ServerDebug("%s  Condition Nbr       : %ld", ind, (int)cc->getConditionNumber());
  ServerDebug("%s  SQL Code            : %ld", ind, (int)cc->getSQLCODE());
  ServerDebug("%s  Server Name         : %s", ind, cc->getServerName());
  ServerDebug("%s  Connection Name     : %s", ind, cc->getConnectionName());
  ServerDebug("%s  Constraint Name     : %s", ind, cc->getConstraintName());
  ServerDebug("%s  Constraint Catalog  : %s", ind, cc->getConstraintCatalog());
  ServerDebug("%s  Constraint Schema   : %s", ind, cc->getConstraintSchema());
  ServerDebug("%s  Catalog Name        : %s", ind, cc->getCatalogName());
  ServerDebug("%s  Schema Name         : %s", ind, cc->getSchemaName());
  ServerDebug("%s  Table Name          : %s", ind, cc->getTableName());
  ServerDebug("%s  Column Name         : %s", ind, cc->getColumnName());
  ServerDebug("%s  Cursor Name         : ", ind, cc->getSqlID());
  ServerDebug("%s  Row Number          : %ld", ind, (int)cc->getRowNumber());

  for (int jj = 0; jj < ComCondition::NumOptionalParms; jj++) {
    if (cc->getOptionalString(jj) != NULL) {
      ServerDebug("%s  OptionalString(%ld) : %s", ind, (int)jj, cc->getOptionalString(jj));
    }

    if (cc->getOptionalInteger(jj) != ComDiags_UnInitialized_Int) {
      ServerDebug("%s  OptionalInteger(%ld): %ld", ind, (int)jj, (int)cc->getOptionalInteger(jj));
    }
  }

}  // dumpComCondition...

void dumpDiagnostics(ComDiagsArea *diags, int indent) {
  ComCondition *cc;
  int ii;
  int nbrW, nbrE, nbrA;

  int indmax;
  if (indent > 99) {
    indmax = 99;
  } else {
    indmax = indent;
  }

  char ind[100];
  int indIdx = 0;
  for (indIdx = 0; indIdx < indmax; indIdx++) ind[indIdx] = ' ';
  ind[indIdx] = '\0';

  nbrW = diags->getNumber(DgSqlCode::WARNING_);
  nbrE = diags->getNumber(DgSqlCode::ERROR_);
  nbrA = diags->getNumber();  // errors followed by warnings...

  ServerDebug(" ");
  ServerDebug("%sContents of Diagnostics Area:", ind);
  ServerDebug("%s-----------------------------", ind);
  ServerDebug("%sMain Error              : %ld", ind, (int)diags->mainSQLCODE());
  ServerDebug("%sNumber                  : %ld", ind, (int)diags->getNumber());
  ServerDebug("%sAre More?               : %s", ind, (diags->areMore() ? "TRUE" : "FALSE"));
  ServerDebug("%sRow Count               : %ld", ind, (int)diags->getRowCount());
  ServerDebug("%sCost                    : %le", ind, (double)diags->getCost());
  ServerDebug("%sAvg Stream Wait Time    : %ld", ind, (int)diags->getAvgStreamWaitTime());
  ServerDebug("%sLength Limit            : %ld", ind, (int)diags->getLengthLimit());
  ServerDebug("%sSQL Function            : %ld", ind, (int)diags->getFunction());
  ServerDebug("%sSQL Function Name       : %s", ind, diags->getFunctionName());
  ServerDebug("%sNbr Warnings            : %ld", ind, (int)nbrW);
  ServerDebug("%sNbr Errors              : %ld", ind, (int)nbrE);

  for (ii = 1; ii <= nbrW; ii++) {
    cc = diags->getWarningEntry(ii);
    ServerDebug("%sWarning(%ld)          :", ind, (int)ii);
    dumpComCondition(cc, ind);
  }

  for (ii = 1; ii <= nbrE; ii++) {
    cc = diags->getErrorEntry(ii);
    ServerDebug("%sError(%ld)            :", ind, (int)ii);
    dumpComCondition(cc, ind);
  }

  ServerDebug(" ");
  return;

  // errors and warnings in order added
  for (ii = 1; ii <= nbrA; ii++) {
    ServerDebug("%sErrors/Warnings(%ld)  :", ind, (int)ii);
    dumpComCondition(cc, ind);
  }

}  // dumpDiagnostics

const char *getDirectionName(ComColumnDirection d) {
  switch (d) {
    case COM_INPUT_COLUMN:
      return "IN";
    case COM_OUTPUT_COLUMN:
      return "OUT";
    case COM_INOUT_COLUMN:
      return "INOUT";
  }
  return ComRtGetUnknownString((int)d);
}

const char *getLmResultSetMode(const LmResultSetMode &rs) {
  switch (rs) {
    case RS_NONE:
      return "RS_NONE";
    case RS_SET:
      return "RS_SET";
  }
  return ComRtGetUnknownString((int)rs);
}

void ServerDebug(const char *formatString, ...) {
  static int pid = getpid();
  fprintf(UdrTraceFile, "[%04d] ", pid);
  va_list args;
  va_start(args, formatString);
  vfprintf(UdrTraceFile, formatString, args);
  fprintf(UdrTraceFile, "\n");
  fflush(UdrTraceFile);
}

void doMessageBox(UdrGlobals *UdrGlob, int trLevel, NABoolean moduleType, const char *moduleName) {
  if (UdrGlob && UdrGlob->traceLevel_ >= trLevel && moduleType) {
    MessageBox(NULL, moduleName, UdrGlob->serverName_, MB_OK | MB_ICONINFORMATION);
  }
}

#ifdef _DEBUG
void sleepIfPropertySet(LmLanguageManager &lm, const char *property, ComDiagsArea *d) {
  int delay = 0;
  if (getLmProperty(lm, property, delay, d) && delay > 0) {
    Sleep(delay * 1000);
  }
}

NABoolean getLmProperty(LmLanguageManager &lm, const char *property, int &result, ComDiagsArea *diags) {
  NABoolean ok = FALSE;
  char buf[100];
  NABoolean isSet = FALSE;
  LmResult r = lm.getSystemProperty(property, buf, 100, isSet, diags);
  if (r == LM_OK && isSet && buf[0] != 0) {
    result = atol(buf);
    ok = TRUE;
  }
  return ok;
}
#endif  // _DEBUG

// This function is used by the signal/trap/exit handler code.
// It is moved from UdrFFDC.cpp file to avoid compilation errors.
// Inclusion of spinfo.h and TFDS header file cause some re-definitions.
void getActiveRoutineInfo(UdrGlobals *UdrGlob, char *routineName, char *routineType, char *routineLanguage,
                          NABoolean &isRoutineActive) {
  SPInfo *currSP = NULL;
  ComRoutineLanguage language;

  routineName[0] = '\0';
  routineType[0] = '\0';
  routineLanguage[0] = '\0';
  isRoutineActive = FALSE;

  currSP = (UdrGlob ? UdrGlob->getCurrSP() : NULL);
  if (currSP == NULL) return;

  strcpy(routineName, currSP->getSqlName());

  language = currSP->getLanguage();

  switch (language) {
    case COM_LANGUAGE_JAVA:
      strcpy(routineLanguage, "JAVA");
      // following is a kluge until we propagate routine type from the plan
      strcpy(routineType, "Stored Procedure");
      break;

    case COM_LANGUAGE_C:
      strcpy(routineLanguage, "C");
      // following is a kluge until we propagate routine type from the plan
      strcpy(routineType, "UDF");
      break;

    case COM_LANGUAGE_CPP:
      strcpy(routineLanguage, "C++");
      // following is a kluge until we propagate routine type from the plan
      strcpy(routineType, "UDF");
      break;

    default:
      break;
  }

  LmLanguageManager *lm = (UdrGlob ? UdrGlob->getLM(language) : NULL);
  if (lm == NULL) return;

  isRoutineActive = lm->isRoutineActive();
}
