#include <iostream>
#include <string>

#include "arkcmp/CmpContext.h"
#include "common/NAMemory.h"
#include "common/charinfo.h"
#include "sqlcomp/parser.h"

using namespace std;

int main(int argc, char const *argv[]) {
  NAHeap *heap = new NAHeap("simulated_context_heap", NAMemory::DERIVED_FROM_SYS_HEAP, 524288, 0);

  CmpContext *context = NULL;
  context = new (heap) CmpContext(CmpContext::IS_DYNAMIC_SQL, heap);

  auto parser = new Parser(context);

  auto sql = "select * from ta"s;
  auto expr = parser->parseDML(sql.c_str(), sql.length(), CharInfo::ISO88591);

  return 0;
}
