#ifndef EXP_IEEE_H
#define EXP_IEEE_H

// Encapsulate IEEE floating point overflow checks

#ifdef __cplusplus
extern "C" {
#endif
double MathReal64Add(double x, double y, short *ov);
double MathReal64Sub(double x, double y, short *ov);
double MathReal64Mul(double x, double y, short *ov);
double MathReal64Div(double x, double y, short *ov);
float MathConvReal64ToReal32(double x, short *ov);
double MathConvReal64ToReal64(double x, short *ov);

#ifdef __cplusplus
}
#endif

void MathEvalException(double result, unsigned long exc, short *ov);  // so exp_eval.cpp can get it

#endif
