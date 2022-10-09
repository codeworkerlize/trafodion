/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ParScannedTokenQueue.h
 * Description:  definitions of non-inline methods for class(es)
 *               ParScannedTokenQueue
 *
 * Created:      5/31/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ParScannedTokenQueue.h"

#include "common/ComOperators.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// definitions of non-inline methods of class ParScannedTokenQueue
// -----------------------------------------------------------------------

//
// constructor
//

ParScannedTokenQueue::ParScannedTokenQueue() : currentPos_(-1) {
  for (int i = 0; i < getQueueSize(); i++) {
    scannedTokens_[i].tokenStrPos = 0;
    scannedTokens_[i].tokenStrLen = 0;
    scannedTokens_[i].tokenStrInputLen = 0;
    scannedTokens_[i].tokenStrOffset = 0;
    scannedTokens_[i].tokenIsComment = FALSE;
  }
}

//
// virtual destructor
//

ParScannedTokenQueue::~ParScannedTokenQueue() {}

//
// mutator
//

void ParScannedTokenQueue::insert(const size_t tokenStrPos, const size_t tokenStrLen, const size_t tokenStrOffset,
                                  NABoolean tokenIsComment) {
  currentPos_ = (currentPos_ + 1) % getQueueSize();
  scannedTokenInfo &tokInfo = scannedTokens_[currentPos_];
  tokInfo.tokenStrPos = tokenStrPos + tokenStrOffset;
  tokInfo.tokenStrLen = tokenStrLen;
  tokInfo.tokenStrInputLen = tokenStrLen;
  tokInfo.tokenStrOffset = tokenStrOffset;
  tokInfo.tokenIsComment = tokenIsComment;
}

void ParScannedTokenQueue::updateInputLen(const size_t tokenStrInputLen) {
  scannedTokenInfo &tokInfo = scannedTokens_[currentPos_];
  tokInfo.tokenStrInputLen = tokenStrInputLen;
}

// accessor

const ParScannedTokenQueue::scannedTokenInfo &ParScannedTokenQueue::getScannedTokenInfo(
    const int tokenInfoIndex) const {
  ComASSERT(tokenInfoIndex <= 0 AND getQueueSize() > -tokenInfoIndex);
  return scannedTokens_[(currentPos_ + getQueueSize() + tokenInfoIndex) % getQueueSize()];
}

ParScannedTokenQueue::scannedTokenInfo *ParScannedTokenQueue::getScannedTokenInfoPtr(const int tokenInfoIndex) {
  ComASSERT(tokenInfoIndex <= 0 AND getQueueSize() > -tokenInfoIndex);
  return &scannedTokens_[(currentPos_ + getQueueSize() + tokenInfoIndex) % getQueueSize()];
}

//
// End of File
//
