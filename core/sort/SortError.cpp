
#include "SortError.h"

#include <stdio.h>
#include <string.h>

#include "common/Platform.h"
#include "common/str.h"

//----------------------------------------------------------------------
// SortError Constructor.
//----------------------------------------------------------------------
SortError::SortError() { initSortError(); }

//----------------------------------------------------------------------
// SortError Destructor.
//----------------------------------------------------------------------
SortError::~SortError() {}

void SortError::initSortError() {
  sortError_ = 0;
  sysError_ = 0;
  sysErrorDetail_ = 0;
  sortErrorMsg_[0] = '\0';
}

void SortError::setErrorInfo(short sorterr, short syserr, short syserrdetail, const char *errorMsg) {
  sortError_ = -sorterr;
  sysError_ = syserr;
  sysErrorDetail_ = syserrdetail;
  if (errorMsg == NULL)
    sortErrorMsg_[0] = '\0';
  else {
    int count = str_len(errorMsg);
    if (count >= sizeof(sortErrorMsg_)) count = sizeof(sortErrorMsg_) - 1;
    str_cpy(sortErrorMsg_, errorMsg, count);
    sortErrorMsg_[count] = '\0';
  }
}

short SortError::getSortError() const { return sortError_; }
short SortError::getSysError() const { return sysError_; }
short SortError::getErrorDetail() const { return sysErrorDetail_; }
char *SortError::getSortErrorMsg() const { return (char *)sortErrorMsg_; }
