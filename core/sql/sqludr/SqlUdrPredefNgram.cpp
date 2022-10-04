/*ngram_udf.cpp*/
#include "sqludr/sqludr.h"
#include "common/ngram.h"

using namespace tmudr;
using namespace std;
#define BUF_LEN 1024
#define TRIGRAM_LEN 3
/* Step 1: derive a class from tmudr::UDR*/

class generate_ngram : public UDR
{
public:
  // determine output columns dynamically at compile time
  void describeParamsAndColumns(UDRInvocationInfo &info);
  
  // override the runtime method
  virtual void processData(UDRInvocationInfo &info,
                           UDRPlanInfo &plan);
};

/* Step 2: Create a factory method*/

extern "C" UDR * GENERATE_NGRAM()
{
  return new generate_ngram();
}

/*  Step 3: Write the actual UDF code*/
void generate_ngram::describeParamsAndColumns(
                           UDRInvocationInfo &info)
{
  // We always expect one input parameter
  if (info.par().getNumColumns() != 1 ||
      info.par().getType(0).getSQLTypeClass() != TypeInfo::CHARACTER_TYPE)
    throw UDRException(38001,
                       "Expecting a single input parameter with a character type");
  info.addFormalParameter(info.par().getColumn(0));
  
  // Make sure we have exactly one table-valued input,
  // otherwise generate a compile error
  if (info.getNumTableInputs() == 1)
  {
    // Make all the input table columns also output columns,
    // those are called "pass-through" columns. The default
    // parameters of this method add all the columns of the
    // first input table.
    info.addPassThruColumns();
    int numOutCols = info.out().getNumColumns();
    int inputTableNum = 0;
    int startInputColNum = 0;
    int endColNum = info.in(inputTableNum).getNumColumns() - 1;
    string colName = info.par().getString(0);

    // this will generate an error if column "colName" is not
    // found in the input table, which is what we want
    int nGramSourceCol = info.in().getColNum(colName);
  }
  else
    info.out().addVarCharColumn("NGRAM", TRIGRAM_LEN);

  // Set the function type, sessionize behaves like
  // a reducer in MapReduce. Session ids are local
  // within rows that share the same id column value.
  info.setFuncType(UDRInvocationInfo::MAPPER);
}
#define BUF_LEN 1024
void generate_ngram::processData(UDRInvocationInfo &info,
                                UDRPlanInfo &plan)
{
  string ngram = info.par().getString(0);
  char srcStr[BUF_LEN];
  TRGM *trg = NULL;
  char tmp[TRIGRAM_LEN+1];
  int trgLen = 0;
  int srcLen = 0;
  int colNum = 0;
  trgm *trgData = NULL;
  if (info.getNumTableInputs() == 1)
  {
    while (getNextRow(info))
    {
      colNum = info.in(0).getColNum(ngram);
      string str = info.in(0).getString(colNum);
      srcLen = str.length();
      memcpy(srcStr, str.data(), srcLen);
      srcStr[srcLen] = '\0';
      trg = generate_trgm(srcStr, srcLen);
      trgLen = ARRNELEM(trg);
      trgData = GETARR(trg);

      for (int i = 0; i < trgLen; i++,trgData++)
      {
        if ( colNum != 0 )
        {
          info.copyPassThruData(0, 0, colNum-1);
        }
        memcpy(tmp, (char *)trgData, TRIGRAM_LEN);
        tmp[TRIGRAM_LEN] = '\0';
        info.out().setString(colNum, tmp, TRIGRAM_LEN);
        // produce the remaining columns and emit the row
        info.copyPassThruData(0, colNum+1);
        emitRow(info);
      }
      if (trg != NULL)
      {
        free(trg);
        trg = NULL;
      }
    }
  }
  else if (info.getNumTableInputs() == 0)
  {
    
    srcLen = ngram.length();
    memcpy(srcStr, ngram.data(), srcLen);
    srcStr[srcLen] = '\0';
    trg = generate_trgm(srcStr, srcLen);
    trgData = GETARR(trg);
    trgLen = ARRNELEM(trg);
    for (int i = 0; i < trgLen; i++,trgData++)
    {
      memcpy(tmp, (char *)trgData, TRIGRAM_LEN);
      tmp[TRIGRAM_LEN] = '\0';
      info.out().setString(0, tmp, TRIGRAM_LEN);
      emitRow(info);
    }
  }
  if (trg != NULL)
    free(trg);
}

/* Step 1: derive a class from tmudr::UDR*/

class generate_wildcard_ngram : public UDR
{
public:

  virtual void describeParamsAndColumns(UDRInvocationInfo &info);
  /* override the runtime method*/
  virtual void processData(UDRInvocationInfo &info,
                           UDRPlanInfo &plan);
};

/* Step 2: Create a factory method*/

extern "C" UDR * GENERATE_WILDCARD_NGRAM()
{
  return new generate_wildcard_ngram();
}

void generate_wildcard_ngram::describeParamsAndColumns(
                           UDRInvocationInfo &info)
{
  if (info.getNumTableInputs() != 0)
    throw UDRException(38001,
                       "UDF %s does not support table-valued inputs",
                       info.getUDRName().c_str());
  // We always expect one input parameter
  if (info.par().getNumColumns() != 1 ||
      info.par().getType(0).getSQLTypeClass() != TypeInfo::CHARACTER_TYPE)
    throw UDRException(38002,
                       "Expecting a single input parameter with a character type");
  info.addFormalParameter(info.par().getColumn(0));
  info.out().addVarCharColumn("NGRAM", TRIGRAM_LEN);
}

/*  Step 3: Write the actual UDF code*/
void generate_wildcard_ngram::processData(UDRInvocationInfo &info,
                                UDRPlanInfo &plan)
{
  string ngram = info.par().getString(0);
  char srcStr[BUF_LEN];
  int srcLen = ngram.length();

  TRGM *trg = generate_wildcard_trgm(ngram.data(), srcLen);
  trgm * trgData = GETARR(trg);
  int trgLen = ARRNELEM(trg);
  char tmp[TRIGRAM_LEN+1];
  for (int i = 0; i < trgLen; i++,trgData++)
  {
    memcpy(tmp, (char *)trgData, TRIGRAM_LEN);
    tmp[TRIGRAM_LEN] = '\0';

    info.out().setString(0, tmp, TRIGRAM_LEN);
    emitRow(info);
  }
  if (trg != NULL)
    free(trg);
}
