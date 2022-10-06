
#ifndef _UDR_FFDC_H_
#define _UDR_FFDC_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         UdrFFDC.h
 * Description:  MXUDR FFDC
 *
 *
 * Created:      5/4/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "common/NABoolean.h"
#include "common/Platform.h"

void comTFDS(const char *msg1, const char *msg2, const char *msg3, const char *msg4, const char *msg5,
             NABoolean dialOut = TRUE, NABoolean writeToSeaLog = TRUE);
void makeTFDSCall(const char *msg, const char *file, UInt32 line, NABoolean dialOut = TRUE,
                  NABoolean writeToSeaLog = TRUE);
void setUdrSignalHandlers();
void printSignalHandlers();
void logEMSWithoutDialOut(const char *msg);
void logEMS(const char *msg);
NABoolean saveUdrTrapSignalHandlers();
NABoolean setSignalHandlersToDefault();
NABoolean restoreJavaSignalHandlers();
NABoolean restoreUdrTrapSignalHandlers(NABoolean saveJavaSignalHandlers);
void setExitHandler();

#endif  // _UDR_FFDC_H_
