
/* AUTOTEXTBUFFER - allocates 5 static char buffers for conversion
   of non textual data (via sprintf) for message inserts    */
#define AUTOTEXTBUFFER 1

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/* for PC and UNIX */
#include "tfds\tfdrtl.h"
#define USER_SEGMENT TFDS_NULL_SEGID

#ifdef TFDS_REGISTRATION_PROC_
#undef TFDS_REGISTRATION_PROC_
#endif

/* ==================   DEFINITION SECTION  =======================  */
#ifdef VPROC_01
#define TFDS_REGISTRATION_PROC_ VPROC(PRODNUMCMP, DATE1CMP, CMP_CC_LABEL)
TFDS_REGPROC_DECLARE(VPROC(PRODNUMCMP, DATE1CMP, CMP_CC_LABEL))
#endif /*  VPROC_01 */

/* ===================    CODE SECTION  ===========================  */

#ifndef TFDS_REGISTRATION_PROC_
#ifdef AUTOTEXTBUFFER
#ifndef TfdsStrBuf
#define TfdsStrBuf
char TfdsStrBuf1[141];
char TfdsStrBuf2[141];
char TfdsStrBuf3[141];
char TfdsStrBuf4[141];
char TfdsStrBuf5[141];
#endif
#endif

#define ProdNum(a)    #a
#define DateCmp(b)    #b
#define CmpCCLabel(c) #c

#define FFDCVprocAsAstring(a, b, c) char NTVprocStringffdc[] = {ProdNum(a)##"_"##DateCmp(b)##"_"##CmpCCLabel(c)}

FFDCVprocAsAstring(PRODNUMCMP, DATE1CMP, CMP_CC_LABEL);

TFDS_REGPROC_DEFINE(VPROC(PRODNUMCMP, DATE1CMP, CMP_CC_LABEL), "Compaq", /* Company Name        */
                    "T1050",                                             /* SQLMXCMP Product Number      */
                    "MXCMP",                                             /* Logical File Name   */
                    "Please see VPROC of MX-Compiler")

#endif /* TFDS_REGISTRATION_PROC */

#ifdef __cplusplus
}
#endif /* __cplusplus */
