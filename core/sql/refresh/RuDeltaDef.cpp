
/* -*-C++-*-
******************************************************************************
*
* File:         RuDeltaDef.cpp
* Description:
*
* Created:      04/04/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "uofsIpcMessageTranslator.h"

#include "RuDeltaDef.h"
#include "RuTbl.h"

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	Constructors and destructor
//--------------------------------------------------------------------------//

CRUUpdateBitmap::CRUUpdateBitmap(int size, const char *buffer)
    : size_(size + 1),  // One byte more for the null terminator
      buffer_(new char[size_]),
      wasChanged_(FALSE) {
  if (NULL == buffer) {
    memset(buffer_, '\0', size_);
  } else {
    memcpy(buffer_, buffer, size_);
  }
}

CRUUpdateBitmap::CRUUpdateBitmap(const CRUUpdateBitmap &other)
    : size_(other.size_), buffer_(new char[size_]), wasChanged_(FALSE) {
  memcpy(buffer_, other.buffer_, size_);
}

CRUUpdateBitmap::~CRUUpdateBitmap() { delete[] buffer_; }

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap::operator =
//--------------------------------------------------------------------------//

CRUUpdateBitmap &CRUUpdateBitmap::operator=(const CRUUpdateBitmap &other) {
  if (this == &other) {
    return *this;
  }

  RUASSERT(size_ == other.size_);
  wasChanged_ = FALSE;

  memcpy(buffer_, other.buffer_, size_);

  return *this;
}

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap::IsNull()
//--------------------------------------------------------------------------//

BOOL CRUUpdateBitmap::IsNull() const {
  for (int i = 0; i < size_; i++) {
    if (0 != buffer_[i]) {
      return FALSE;
    }
  }

  return TRUE;
}

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap::Reset()
//--------------------------------------------------------------------------//

void CRUUpdateBitmap::Reset() {
  wasChanged_ = FALSE;
  memset(buffer_, '\0', size_);
}

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap::operator |=
//--------------------------------------------------------------------------//

CRUUpdateBitmap &CRUUpdateBitmap::operator|=(const CRUUpdateBitmap &other) {
  RUASSERT(size_ == other.size_);

  for (int i = 0; i < size_; i++) {
    if (buffer_[i] != other.buffer_[i]) {
      wasChanged_ = TRUE;
      buffer_[i] |= other.buffer_[i];
    }
  }

  return *this;
}

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap::CreateInstance()
//
//	Create a new class instance from the stream. The method is different
//	from the traditional LoadData() because the buffer's size is a part
//	of the serialized data. So, the alternative was to create a size-less
//	(and buffer-less) instance of the class first, which is ugly.
//
//--------------------------------------------------------------------------//

CRUUpdateBitmap *CRUUpdateBitmap::CreateInstance(CUOFsIpcMessageTranslator &translator) {
  int size;

  translator.ReadBlock(&size, sizeof(int));
  RUASSERT(size > 0);

  char *buffer = new char[size];
  translator.ReadBlock(buffer, size);

  CRUUpdateBitmap *pUpdateBitmap = new CRUUpdateBitmap(size, buffer);

  delete[] buffer;

  return pUpdateBitmap;
}

//--------------------------------------------------------------------------//
//	CRUUpdateBitmap::StoreData()
//
//	Serialize the context
//--------------------------------------------------------------------------//
void CRUUpdateBitmap::StoreData(CUOFsIpcMessageTranslator &translator) {
  RUASSERT(size_ > 0);

  translator.WriteBlock(&size_, sizeof(int));
  translator.WriteBlock(buffer_, size_);
}

//--------------------------------------------------------------------------//
//	CRUDeltaDef
//--------------------------------------------------------------------------//

CRUDeltaDef::CRUDeltaDef(CRUTbl *pTbl)
    : tblUid_(pTbl->GetUID()),
      tblName_(pTbl->GetFullName()),
      fromEpoch_(0),
      toEpoch_(pTbl->GetCurrentEpoch()),
      deLevel_(NO_DE),
      isRangeLogNonEmpty_(FALSE),
      isIUDLogNonEmpty_(FALSE),
      isIUDLogInsertOnly_(pTbl->IsInsertLog()),
      pStat_(NULL) {}

CRUDeltaDef::~CRUDeltaDef() { delete pStat_; }

//--------------------------------------------------------------------------//
//	CRUDeltaDefList
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUDeltaDefList::FindByUID()
//--------------------------------------------------------------------------//

CRUDeltaDef *CRUDeltaDefList::FindByUID(TInt64 tblUid) const {
  CRUDeltaDef *pDdef = NULL;

  DSListPosition pos = GetHeadPosition();
  while (NULL != pos) {
    pDdef = GetNext(pos);
    if (tblUid == pDdef->tblUid_) {
      break;
    }
  }

  return pDdef;
}

//--------------------------------------------------------------------------//
//	CRUDeltaDefList::RemoveByUID()
//--------------------------------------------------------------------------//

void CRUDeltaDefList::RemoveByUID(TInt64 tblUid) {
  DSListPosition prevpos = NULL;
  DSListPosition pos = GetHeadPosition();

  for (;;) {
    prevpos = pos;
    CRUDeltaDef *pDdef = GetNext(pos);

    if (tblUid == pDdef->tblUid_) {
      if (NULL == prevpos) {
        RemoveHead();
      } else {
        RemoveAt(prevpos);
      }

      return;
    }
  }

  // The delta-def should have been in the list
  RUASSERT(FALSE);
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatistics
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	Constructors and destructor
//--------------------------------------------------------------------------//

CRUDeltaStatistics::CRUDeltaStatistics()
    :  // Range log statistics
      nRanges_(0),
      nRangeCoveredRows_(0),
      // Exact IUD log statistics
      nInsertedRows_(0),
      nDeletedRows_(0),
      nUpdatedRows_(0),
      pUpdateBitmap_(NULL) {}

CRUDeltaStatistics::CRUDeltaStatistics(const CRUDeltaStatistics &other)
    : nRanges_(other.nRanges_),
      nRangeCoveredRows_(other.nRangeCoveredRows_),
      nInsertedRows_(other.nInsertedRows_),
      nDeletedRows_(other.nDeletedRows_),
      nUpdatedRows_(other.nUpdatedRows_),
      pUpdateBitmap_(NULL) {
  CRUUpdateBitmap *pOtherUpdateBitmap = other.pUpdateBitmap_;
  if (NULL != pOtherUpdateBitmap) {
    pUpdateBitmap_ = new CRUUpdateBitmap(*pOtherUpdateBitmap);
  }
}

CRUDeltaStatistics::~CRUDeltaStatistics() { delete pUpdateBitmap_; }

//--------------------------------------------------------------------------//
//	CRUDeltaStatistics::operator =
//--------------------------------------------------------------------------//

CRUDeltaStatistics &CRUDeltaStatistics::operator=(const CRUDeltaStatistics &other) {
  if (this == &other) {
    return *this;
  }

  nRanges_ = other.nRanges_;
  nRangeCoveredRows_ = other.nRangeCoveredRows_;

  nInsertedRows_ = other.nInsertedRows_;
  nDeletedRows_ = other.nDeletedRows_;
  nUpdatedRows_ = other.nUpdatedRows_;

  if (NULL != pUpdateBitmap_) {
    delete pUpdateBitmap_;
    pUpdateBitmap_ = NULL;
  }

  CRUUpdateBitmap *pOtherUpdateBitmap = other.pUpdateBitmap_;
  if (NULL != pOtherUpdateBitmap) {
    pUpdateBitmap_ = new CRUUpdateBitmap(*pOtherUpdateBitmap);
  }

  return *this;
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatistics::GetDeltaSize()
//
//	Delta size estimate (for the Refresh task)
//--------------------------------------------------------------------------//

TInt32 CRUDeltaStatistics::GetDeltaSize() {
  TInt64 size = nInsertedRows_ + nDeletedRows_ + nUpdatedRows_;
  if (RANGE_SIZE_UNKNOWN != nRangeCoveredRows_) {
    size += nRangeCoveredRows_;
  }

  return (size < MAX_STATISTIC) ? (TInt32)size : MAX_STATISTIC;
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatistics::LoadData()
//
//	De-serialize the context
//--------------------------------------------------------------------------//
void CRUDeltaStatistics::LoadData(CUOFsIpcMessageTranslator &translator) {
  translator.ReadBlock(&nRanges_, sizeof(TInt32));
  translator.ReadBlock(&nRangeCoveredRows_, sizeof(TInt32));

  translator.ReadBlock(&nInsertedRows_, sizeof(TInt32));
  translator.ReadBlock(&nDeletedRows_, sizeof(TInt32));
  translator.ReadBlock(&nUpdatedRows_, sizeof(TInt32));

  if (NULL != pUpdateBitmap_) {
    delete pUpdateBitmap_;
    pUpdateBitmap_ = NULL;
  }

  BOOL flag;
  translator.ReadBlock(&flag, sizeof(BOOL));

  if (TRUE == flag) {
    // There is a serialized bitmap, create a new instance
    pUpdateBitmap_ = CRUUpdateBitmap::CreateInstance(translator);
  }
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatistics::StoreData()
//
//	Serialize the context
//--------------------------------------------------------------------------//

void CRUDeltaStatistics::StoreData(CUOFsIpcMessageTranslator &translator) {
  translator.WriteBlock(&nRanges_, sizeof(TInt32));
  translator.WriteBlock(&nRangeCoveredRows_, sizeof(TInt32));

  translator.WriteBlock(&nInsertedRows_, sizeof(TInt32));
  translator.WriteBlock(&nDeletedRows_, sizeof(TInt32));
  translator.WriteBlock(&nUpdatedRows_, sizeof(TInt32));

  BOOL flag;
  if (NULL == pUpdateBitmap_) {
    flag = FALSE;
    translator.WriteBlock(&flag, sizeof(BOOL));
  } else {
    flag = TRUE;
    translator.WriteBlock(&flag, sizeof(BOOL));
    pUpdateBitmap_->StoreData(translator);
  }
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatistics::GetPackedBufferSize()
//
//	Room required for the serialized buffer
//--------------------------------------------------------------------------//

TInt32 CRUDeltaStatistics::GetPackedBufferSize(int updateBitmapSize) {
  return sizeof(TInt32)          // nRanges_
         + sizeof(TInt32)        // nRangeCoveredRows_
         + sizeof(TInt32)        // nInsertedRows_
         + sizeof(TInt32)        // nDeletedRows_
         + sizeof(TInt32)        // nUpdatedRows_
         + sizeof(int)         // update bitmap buffer size
         + updateBitmapSize + 1  // update bitmap buffer
      ;
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatisticsMap
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUDeltaStatisticsMap::operator =
//--------------------------------------------------------------------------//

CRUDeltaStatisticsMap &CRUDeltaStatisticsMap::operator=(const CRUDeltaStatisticsMap &other) {
  CRUDeltaStatistics deStat;
  int epoch;

  CDSMapPosition<CRUDeltaStatistics> pos;
  other.GetStartPosition(pos);

  while (TRUE == pos.IsValid()) {
    other.GetNextAssoc(pos, epoch, deStat);
    (*this)[epoch] = deStat;
  }

  return *this;
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatisticsMap::LoadData()
//
//	De-serialize the context
//--------------------------------------------------------------------------//
void CRUDeltaStatisticsMap::LoadData(CUOFsIpcMessageTranslator &translator) {
  CRUDeltaStatistics deStat;
  int count, epoch;

  translator.ReadBlock(&count, sizeof(int));
  RUASSERT(0 == this->GetCount() && count > 0);

  for (int i = 0; i < count; i++) {
    translator.ReadBlock(&epoch, sizeof(int));
    deStat.LoadData(translator);
    (*this)[epoch] = deStat;
  }
}

//--------------------------------------------------------------------------//
//	CRUDeltaStatisticsMap::StoreData()
//
//	Serialize the context
//--------------------------------------------------------------------------//
void CRUDeltaStatisticsMap::StoreData(CUOFsIpcMessageTranslator &translator) {
  CRUDeltaStatistics deStat;
  int epoch;

  int count = this->GetCount();
  RUASSERT(count > 0);

  translator.WriteBlock(&count, sizeof(int));

  CDSMapPosition<CRUDeltaStatistics> pos;
  this->GetStartPosition(pos);

  while (TRUE == pos.IsValid()) {
    this->GetNextAssoc(pos, epoch, deStat);

    translator.WriteBlock(&epoch, sizeof(int));
    deStat.StoreData(translator);
  }
}
