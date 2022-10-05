/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      4/15/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "generator/Generator.h"
#include "optimizer/RelExpr.h"

short RelExpr::codeGen(Generator *generator) {
  // well, sometimes it reaches here. Just codeGen the kids and return.
  for (short i = 0; i < getArity(); i++) child(i)->codeGen(generator);

  // return what you got.
  generator->setCriDesc((ex_cri_desc *)(generator->getCriDesc(Generator::DOWN)), Generator::UP);

  return 0;
}
