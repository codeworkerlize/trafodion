
#ifndef _RU_DUPELIM_SINGLE_ROW_RESOLV_H_
#define _RU_DUPELIM_SINGLE_ROW_RESOLV_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuDupElimSingleRowResolv.h
* Description:  Definition of class CRUDupElimSingleRowResolver
*
*
* Created:      07/02/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "RuDupElimTaskExUnit.h"
#include "RuDeltaDef.h"

class CRUDupElimLogScanner;
class CRUIUDLogRecord;

//--------------------------------------------------------------------------//
//	CRUDupElimSingleRowResolver
//
//	This class resolves duplicate conflicts between single-row records.
//	This kind of resolution is required only if the	table has an MJV
//	"customer". All the other conflicts are handled by CRUDupElimRangeResolver.
//
//	The decisions are translated into the Update and Delete
//	operations applied to the single-row records in the IUD log.
//
//	The single-row resolver allows to complete the DE phase (and commit
//	the transaction) only on the duplicate chain boundary (i.e, the records
//	that relate to the same clustering key are always processed in the same
//	phase).
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUDupElimSingleRowResolver : public CRUDupElimResolver {
 private:
  typedef CRUDupElimResolver inherited;

 public:
  CRUDupElimSingleRowResolver(const CRUDupElimGlobals &globals, CRUSQLDynamicStatementContainer &ctrlStmtContainer);

  virtual ~CRUDupElimSingleRowResolver() {}

  // Implementation of pure virtuals
 public:
  //------------------------------------//
  //	Accessors
  //------------------------------------//
#ifdef _DEBUG
  virtual void DumpPerformanceStatistics(CDSString &to) const;
#endif

 public:
  //------------------------------------//
  //	Mutators
  //------------------------------------//
  virtual void Reset();

  //-- Resolve the next duplicate conflict (including the write to disk).
  virtual void Resolve(CRUDupElimLogScanner &scanner);

  // Prepare all the SQL statements in the container (together)
  virtual void PrepareSQL();

  // IPC pack/unpack
  virtual void LoadReply(CUOFsIpcMessageTranslator &translator);
  virtual void StoreReply(CUOFsIpcMessageTranslator &translator);

  //------------------------------------//
  //	PRIVATE AREA
  //------------------------------------//
 private:
  // The update bitmap common throughout the chain
  CRUUpdateBitmap updateBitmap_;

  // Is there at least one update in this chain?
  BOOL isUpdateRecInChain_;
  // Are all records in this chain updates?
  BOOL isPureUpdateChain_;

  // Statistics for the number of performed IUD statements
  int numDeleteRecord_;
  int numUpdateIgnMark_;
  int numUpdateBitmap_;
  int numUpdateOpType_;

 private:
  //-- Prevent copying
  CRUDupElimSingleRowResolver(const CRUDupElimSingleRowResolver &other);
  CRUDupElimSingleRowResolver &operator=(const CRUDupElimSingleRowResolver &other);

 private:
  // Resolve() callees
  void EndChain(CRUIUDLogRecord *pLastRecInChain);
  void HandleUpdateBitmapAtEndOfChain(CRUIUDLogRecord *pLastRecInChain);

  void ResolveInsert(CRUIUDLogRecord *pPrevRec);
  void ResolveDelete(CRUIUDLogRecord *pCurrentRec, CRUIUDLogRecord *pPrevRec);

  void MonitorUpdate(CRUIUDLogRecord *pCurrentRec);

  // Execution of U/D operations on the IUD log
  void ExecuteDeleteRecord(CRUIUDLogRecord *pRec);
  void ExecuteUpdateIgnMark(CRUIUDLogRecord *pRec);

  void ExecuteUpdateBitmapInChain(CRUIUDLogRecord *pLastRecInChain);
  void ExecuteUpdateOpTypeInChain(CRUIUDLogRecord *pLastRecInChain);
};

#endif
