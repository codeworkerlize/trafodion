/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ComRegAPI.h
 * Description:  Common routines used to access system APIs.
 *
 * Created:      10/08/97
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#ifndef COMREGAPI_H_
#define COMREGAPI_H_

#include "common/ComSmallDefs.h"
#include "common/Platform.h"

// log the error message in the event log.
void logErrorMessageInEventLog(int msgId, const char *msg);

// retrieve the Tandem volume.
ComString getTandemSysVol();

// retrieve the Tandem metadata versions.
ComString getTandemMetaDataVersions();

#endif  // COMREGAPI_H_
