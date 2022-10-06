
/* -*-C++-*-
****************************************************************************
*
* File:         SequenceGeneratorAttributes.cpp
* Description:  The attributes of the sequence generator
* Created:
* Language:     C++
*
****************************************************************************/

#include "common/SequenceGeneratorAttributes.h"

#include "common/ComSpace.h"

void SequenceGeneratorAttributes::genSequenceName(const NAString &catName, const NAString &schName,
                                                  const NAString &tabName, const NAString &colName, NAString &seqName) {
  seqName = "_" + catName + "_" + schName + "_" + tabName + "_" + colName + "_";
}

const void SequenceGeneratorAttributes::display(ComSpace *space, NAString *nas,
                                                NABoolean noNext,    /*do not show next value*/
                                                NABoolean isShowDDL, /*for m12357*/
                                                NABoolean commentOut /*comment out internal seq*/) const {
  char buf[10000] = "";
  char *bufptr = buf;
  if ((isShowDDL) && (isSystemSG())) {
    sprintf(buf, "  GLOBAL  TIMEOUT %ld", getSGTimeout());
    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
  }

  // for mantis 12375
  // start with means the next value to start with
  if (!isSystemSG()) {
    bufptr = buf;
    if (commentOut) {
      memcpy(bufptr, "--", 2);
      bufptr = buf + 2;
    }
    if (isShowDDL)
      sprintf(bufptr, "  START WITH %li", getSGNextValue());
    else {
      if (noNext)
        sprintf(bufptr, "  START WITH %li", getSGStartValue());
      else
        sprintf(bufptr, "  START WITH %li /* NEXT AVAILABLE VALUE %li */", getSGStartValue(), getSGNextValue());
    }

    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
  }
  if (!isSystemSG()) {
    bufptr = buf;
    if (commentOut) {
      memcpy(bufptr, "--", 2);
      bufptr = buf + 2;
    }
    sprintf(bufptr, "  INCREMENT BY %li", getSGIncrement());
    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
  }

  bufptr = buf;
  if (commentOut && !isSystemSG()) {
    memcpy(bufptr, "--", 2);
    bufptr = buf + 2;
  }
  if (getSGMaxValue() == 0)
    sprintf(bufptr, "  NO MAXVALUE");
  else
    sprintf(bufptr, "  MAXVALUE %li", getSGMaxValue());

  if (nas)
    *nas += buf;
  else
    space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));

  if (!isSystemSG()) {
    bufptr = buf;
    if (commentOut) {
      memcpy(bufptr, "--", 2);
      bufptr = buf + 2;
    }
    if (getSGMinValue() == 0)
      sprintf(bufptr, "  NO MINVALUE");
    else
      sprintf(bufptr, "  MINVALUE %li", getSGMinValue());

    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
  }
  if (!isSystemSG()) {
    bufptr = buf;
    if (commentOut) {
      memcpy(bufptr, "--", 2);
      bufptr = buf + 2;
    }

    if (getSGCache() == 0)
      sprintf(bufptr, "  NO CACHE");
    else
      sprintf(bufptr, "  CACHE %li", getSGCache());
    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));

    bufptr = buf;
    if (commentOut) {
      memcpy(bufptr, "--", 2);
      bufptr = buf + 2;
    }
    if (getSGOrder() == 0)
      sprintf(bufptr, "  NOORDER");
    else
      sprintf(bufptr, "  ORDER");

    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));

    bufptr = buf;
    if (commentOut) {
      memcpy(bufptr, "--", 2);
      bufptr = buf + 2;
    }
    if (getSGCycleOption())
      sprintf(bufptr, "  CYCLE");
    else
      sprintf(bufptr, "  NO CYCLE");

    if (nas)
      *nas += buf;
    else
      space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));

    if (getSGSyncRepl()) {
      bufptr = buf;
      if (commentOut) {
        memcpy(bufptr, "--", 2);
        bufptr = buf + 2;
      }
      sprintf(bufptr, "  SYNCHRONOUS REPLICATION ");
      if (nas)
        *nas += buf;
      else
        space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
    } else if (getSGAsyncRepl()) {
      bufptr = buf;
      if (commentOut) {
        memcpy(bufptr, "--", 2);
        bufptr = buf + 2;
      }
      sprintf(bufptr, "  ASYNCHRONOUS REPLICATION ");
      if (nas)
        *nas += buf;
      else
        space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
    }
  }

  bufptr = buf;
  if (commentOut && !isSystemSG()) {
    memcpy(bufptr, "--", 2);
    bufptr = buf + 2;
  }
  if (getSGFSDataType() == COM_UNSIGNED_BIN16_FSDT)
    sprintf(bufptr, "  SMALLINT UNSIGNED ");
  else if (getSGFSDataType() == COM_UNSIGNED_BIN32_FSDT)
    sprintf(bufptr, "  INT UNSIGNED ");
  else
    sprintf(bufptr, "  LARGEINT ");

  if (nas)
    *nas += buf;
  else
    space->allocateAndCopyToAlignedSpace(buf, strlen(buf), sizeof(short));
}
