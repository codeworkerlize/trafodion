
#ifndef UDRERRORS_H
#define UDRERRORS_H
/* -*-C++-*-
******************************************************************************
*
* File:         UdrErrors.h
* Description:  Errors/Warnings generated in SQL compiler for UDR's
*		(parser/binder/normalizer/optimizer)
*
* Created:      1/02/2001
* Language:     C++
*
*
******************************************************************************
*/

enum UDRErrors {
  UDR_CATMAN_FIRST_ERROR = 1300,
  UDR_CATMAN_EXCEPTION = UDR_CATMAN_FIRST_ERROR,
  UDR_BINDER_FIRST_ERROR = 4300,
  UDR_BINDER_OUTPARAM_IN_TRIGGER = UDR_BINDER_FIRST_ERROR,
  UDR_BINDER_RESULTSETS_IN_TRIGGER = 4301,
  UDR_BINDER_INCORRECT_PARAM_COUNT = 4302,
  UDR_BINDER_PARAM_TYPE_MISMATCH = 4303,
  UDR_BINDER_MULTI_HOSTVAR_OR_DP_IN_PARAMS = 4304,
  UDR_BINDER_OUTVAR_NOT_HV_OR_DP = 4305,
  UDR_BINDER_SP_IN_COMPOUND_STMT = 4306,
  UDR_BINDER_NO_ROWSET_IN_CALL = 4307,
  UDR_BINDER_UNSUPPORTED_TYPE = 4308,
  UDR_BINDER_RESULTSETS_NOT_ALLOWED = 4309,
  UDR_BINDER_PROC_LABEL_NOT_ACCESSIBLE = 4338
};

#endif
