#include "sqludr.h"
#include <string.h>

SQLUDR_LIBFUNC SQLUDR_INT32 test002_func_char (
  SQLUDR_CHAR *in,
  SQLUDR_CHAR *out,
  SQLUDR_INT16 *inInd,
  SQLUDR_INT16 *outInd,
  SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;

  if (SQLUDR_GETNULLIND(inInd) == SQLUDR_NULL)
    SQLUDR_SETNULLIND(outInd);
  else
    memcpy (out, in, udrinfo->inputs->data_len);

  return SQLUDR_SUCCESS;
}


SQLUDR_LIBFUNC SQLUDR_INT32 test002_func_vcstruct (
  SQLUDR_VC_STRUCT *in,
  SQLUDR_VC_STRUCT *out,
  SQLUDR_INT16 *inInd,
  SQLUDR_INT16 *outInd,
  SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;

  if (SQLUDR_GETNULLIND(inInd) == SQLUDR_NULL)
    SQLUDR_SETNULLIND(outInd);
  else
  {
    memcpy (out->data, in->data, in->length);
    out->length = in->length;
  }

  return SQLUDR_SUCCESS;
}
