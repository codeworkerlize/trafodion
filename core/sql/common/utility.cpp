#include "utility.h"
#include <math.h>

Int32 smallestFactor(Int32 x) {
  // A very basic implementation.
  // To be optimized.
  for (Int32 i = 2; i < x; i++) {
    if (x % i == 0) return i;
  }

  return x;
}

Int32 largestFactor(Int32 x) {
  // A very basic implementation.
  // To be optimized.
  for (Int32 i = x - 1; i >= 2; i--) {
    if (x % i == 0) return i;
  }

  return x;
}

// Return
//    y, when y <= x || y <= z
//    a factor of y and a multiple of z that is closest to x, otherwise
Int32 closestFactor(Int32 x, Int32 y, Int32 z) {
  if (x >= y || z >= y) return y;

  // When reach here: 1 <= x <= y
  Int32 i, j;
  // find a factor i in [x, y-1], i%z == 0
  for (i = x; i < y; i++) {
    if (y % i == 0 && (z <= 1 || i % z == 0)) break;
  }

  // find a factor j in [1, x-1], j%z == 0
  for (j = x - 1; j >= 1; j--) {
    if (y % j == 0 && (z <= 1 || j % z == 0)) break;
  }

  // if j==1, then there is no factor smaller than x
  // we must use i.
  if (j == 1) return i;

  // j is a valid factor. Check i
  // If i == y, it means there is no factor in range
  // [x, y-1], we must use j
  if (i == y) return j;

  // now both i and j are factors, decide which one
  // is closer to x
  return ((x - j) < (i - x)) ? j : i;
}

// return a value that is a multple of y and closest to x.
// If x is less than y, y is returned.
Int32 adjustToMultiple(Int32 x, Int32 y) {
  if (x < y) return y;

  Int32 c1 = y * floor(x / y);
  Int32 c2 = c1 + y;

  if (x - c1 < c2 - x)
    return c1;
  else
    return c2;
}
