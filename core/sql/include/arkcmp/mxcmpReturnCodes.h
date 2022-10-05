
#ifndef __MXCMPRETURNCODES_H
#define __MXCMPRETURNCODES_H

/* -*-C++-*-
 *****************************************************************************
 * File:         mxcmpReturnCodes.h
 * Description:  This holds the mxcmp exit codes.
 * Created:      10/23/2003
 * Language:     C++
 *****************************************************************************
 */

// mxcmp & mxCompileUserModule need these constants so that
// they can both return the same exit codes to ETK.

enum mxcmpExitCode { SUCCEED = 0, FAIL = 1, FAIL_RETRYABLE = -1, ERROR = 2, WARNING = 3 };

#endif /* __MXCMPRETURNCODES_H */
