
/* -*-C++-*-
****************************************************************************
*
* File:         StoredProcInterface.h (previosuly /executor/ExSPInterface.h)
* Description:  Interface file to ARKCMP for SP related stuff
*
* Created:      5/6/98
* Language:     C++
*
*
*
****************************************************************************
*/

#ifndef SP_INTERFACE_H_
#define SP_INTERFACE_H_

#include "common/BaseTypes.h"


class ComDiagsArea;

/////////////////////////////////////////////////////////////
// See executor/ex_stored_proc.cpp for details about these
// procs...
////////////////////////////////////////////////////////////
short ExSPPrepareInputBuffer(void *inputBuffer);

short ExSPPosition(void *inputBuffer);

short ExSPGetInputRow(void *inputBuffer,   // IN:  input sql buffer
                      void *&controlInfo,  // OUT: control info
                      char *&rowPtr,       // OUT: pointer to the row
                      int &rowLen);     // OUT: length of returned row

short ExSPInitReplyBuffer(void *replyBuffer, int replyBufLen);

short ExSPPutReplyRow(void *replyBuffer,         // IN: the reply buffer
                      void *controlInfo,         // IN: control info
                      char *replyRow,            // IN: pointer to reply row
                      int rowLen,             // IN: length of reply row
                      ComDiagsArea *diagsDesc);  // IN: pointer to diags

short ExSPPrepareReplyBuffer(void *replyBuffer);

short ExSPUnpackIOExpr(void *&extractInputExpr, void *&moveOutputExpr, CollHeap *heap);

short ExSPExtractInputValue(void *extractInputExpr, int fieldNum, char *inputRow, char *data, int datalen,
                            NABoolean casting,  // if TRUE,data in varchar, to be casted
                            ComDiagsArea *diagsArea);

short ExSPMoveOutputValue(void *moveOutputExpr, int fieldNum, char *outputRow, char *data, int datalen,
                          NABoolean casting,  // if TRUE, data in varchar, to be casted
                          ComDiagsArea *diagsArea, CollHeap *heap);

#endif
