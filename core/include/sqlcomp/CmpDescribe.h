
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Describe.C
 * Description:
 *
 * Created:      4/15/95
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *****************************************************************************
 */

#ifndef __CMP_DESCRIBE_H
#define __CMP_DESCRIBE_H

#include "common/ComVersionDefs.h"
#include "common/NABoolean.h"

class ExeCliInterface;

short exeImmedOneStmt(const char *stmt);

short sendAllControls(NABoolean copyCQS, NABoolean sendAllCQDs, NABoolean sendUserCQDs,
                      enum COM_VERSION versionOfCmplrRcvCntrlInfo = COM_VERS_COMPILER_VERSION,
                      NABoolean sendUserCSs = TRUE, CmpContext *prevContext = NULL);

void sendParserFlag(int flag);

short setParentQidAtSession(NAHeap *heap, const char *parentQid);

extern short CmpDescribeSeabaseTable(const CorrName &dtName,
                                     short type,  // 1, invoke. 2, showddl. 3, createLike
                                     char *&outbuf, int &outbuflen, CollHeap *heap, NABoolean isCreatePartition = FALSE,
                                     const char *pkeyName = NULL, const char *pkeyStr = NULL,
                                     NABoolean withPartns = FALSE, NABoolean withoutSalt = FALSE,
                                     NABoolean withoutDivisioning = FALSE, NABoolean withoutRowFormat = FALSE,
                                     NABoolean withoutLobColumns = FALSE, NABoolean withoutNamespace = FALSE,
                                     NABoolean withoutRegionReplication = FALSE, NABoolean withoutIncrBackup = FALSE,
                                     UInt32 columnLengthLimit = UINT_MAX, NABoolean noTrailingSemi = FALSE,
                                     NABoolean noPrivs = FALSE,

                                     // used to add,rem,alter column definition from col list.
                                     // valid for 'createLike' mode.
                                     // Used for 'alter add/drop/alter col'.
                                     char *colName = NULL,
                                     short ada = 0,  // 0,add. 1,drop. 2,alter
                                     const NAColumn *nacol = NULL, const NAType *natype = NULL, Space *inSpace = NULL,
                                     NABoolean isDetail = FALSE, CorrName *likeTabName = NULL, short subType = 0);

// type:  1, invoke. 2, showddl. 3, create_like
extern short cmpDisplayColumn(const NAColumn *nac, char *inColName, const NAType *inNAT, short displayType,
                              Space *inSpace, char *buf, int &ii, NABoolean namesOnly, NABoolean &identityCol,
                              NABoolean isExternalTable, NABoolean isAlignedRowFormat, UInt32 columnLengthLimit,
                              NAList<const NAColumn *> *truncatedColumnList);

extern short cmpDisplayPrimaryKey(const NAColumnArray &naColArr, int numKeys, NABoolean displaySystemCols, Space &space,
                                  char *buf, NABoolean displayCompact, NABoolean displayAscDesc,
                                  NABoolean displayParens);

extern short CmpGenUniqueConstrStr(AbstractRIConstraint *ariConstr, Space *space, char *buf, NABoolean outputLine,
                                   NABoolean unnamed = FALSE, const CorrName *likeCorrName = NULL);

extern short CmpGenRefConstrStr(AbstractRIConstraint *ariConstr, Space *space, char *buf, NABoolean outputLine,
                                NABoolean unnamed = FALSE, const CorrName *likeCorrName = NULL);

#endif
