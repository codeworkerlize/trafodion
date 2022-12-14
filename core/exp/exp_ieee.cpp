
#include "exp_ieee.h"

#include <fenv.h>
#include <math.h>

void CLEAREXCEPT() { feclearexcept(FE_OVERFLOW | FE_INVALID | FE_DIVBYZERO | FE_UNDERFLOW); }

void GETEXCEPT(unsigned long *mxcsr) { *mxcsr = fetestexcept(FE_UNDERFLOW | FE_OVERFLOW | FE_DIVBYZERO | FE_INVALID); }

void MathEvalExceptionConv1(double result, unsigned long exc, short *ov) {
  if (exc & (FE_OVERFLOW | FE_INVALID | FE_DIVBYZERO))
    *ov = 1;
  else if ((exc & FE_UNDERFLOW) && (result == 0))
    *ov = -1;
}

void MathEvalExceptionConv2(unsigned long exc, short *ov) {
  if (exc & (FE_OVERFLOW | FE_INVALID | FE_DIVBYZERO))
    *ov = 1;
  else if (exc & FE_UNDERFLOW)
    *ov = -1;
}

void MathEvalException(double result, unsigned long exc, short *ov) {
  if (exc & (FE_OVERFLOW | FE_INVALID | FE_DIVBYZERO))
    *ov = 1;
  else if ((exc & FE_UNDERFLOW) && (result == 0))
    *ov = 1;
}

/*************************************************************************/
/*          arithmetic functions involving floating point                */
/*************************************************************************/

// the next few 'static' methods *must* be separate procedures.
// They should not be made inline, or moved into the calling methods.
// This is needed so the float operations are actually done when these
// methods are called which will set the float exception bits that
// the caller rely upons. If these are made inline, then optimize 2
// compile may move them around so these operations are not done at
// the place where they are called.
double doReal64Add(double op1, double op2) { return (op1 + op2); }
double doReal64Sub(double op1, double op2) { return (op1 - op2); }
double doReal64Mul(double op1, double op2) { return (op1 * op2); }
double doReal64Div(double op1, double op2) { return (op1 / op2); }
float doConvReal64ToReal32(double op1) { return (float)op1; }

double doConvReal64ToReal64(double op1) {
  double temp = op1;
  return temp;
}

// Note: A copy of this function's logic has been inlined
// in exp_eval.cpp. So if you change this, change that logic
// also. (We may ultimately want to inline this function
// everywhere, or even all the functions in this module, but
// that requires a more complete study of performance impacts
// than I am prepared to invest at the moment.)

double MathReal64Add(double x, double y, short *ov) {
  *ov = 0;
  unsigned long exc = 0;
  CLEAREXCEPT();

  double res = doReal64Add(x, y);

  GETEXCEPT(&exc);
  if (exc) {
    MathEvalException(res, exc, ov);
  }
  return res;
}

double MathReal64Sub(double x, double y, short *ov) {
  *ov = 0;
  unsigned long exc = 0;
  CLEAREXCEPT();

  double res = doReal64Sub(x, y);

  GETEXCEPT(&exc);
  if (exc) {
    MathEvalException(res, exc, ov);
  }
  return res;
}

double MathReal64Mul(double x, double y, short *ov) {
  *ov = 0;
  unsigned long exc = 0;
  CLEAREXCEPT();

  double res = doReal64Mul(x, y);

  GETEXCEPT(&exc);
  if (exc) {
    MathEvalException(res, exc, ov);
  }
  return res;
}

double MathReal64Div(double x, double y, short *ov) {
  *ov = 0;
  unsigned long exc = 0;
  CLEAREXCEPT();

  double res = doReal64Div(x, y);

  GETEXCEPT(&exc);
  if (exc) {
    MathEvalException(res, exc, ov);
  }
  return res;
}

double MathConvReal64ToReal64(double x, short *ov) {
  *ov = 0;
  unsigned long exc = 0;
  CLEAREXCEPT();

  double res = doConvReal64ToReal64(x);

  GETEXCEPT(&exc);
  if (exc) {
    MathEvalExceptionConv2(exc, ov);
  }
  return res;
}

float MathConvReal64ToReal32(double x, short *ov) {
  *ov = 0;
  unsigned long exc = 0;
  CLEAREXCEPT();

  float res = doConvReal64ToReal32(x);

  GETEXCEPT(&exc);
  if (exc) {
    MathEvalExceptionConv1(res, exc, ov);
  }
  return res;
}
