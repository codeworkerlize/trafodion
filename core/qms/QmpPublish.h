// **********************************************************************

// **********************************************************************

#ifndef _QMPPUBLISH_H_
#define _QMPPUBLISH_H_

#include <fstream>

#include "qmscommon/QRMessage.h"
#include "QRQueriesImpl.h"
#include "common/CollHeap.h"
#include "qmscommon/QRDescriptor.h"

enum PublishTarget { PUBLISH_TO_QMM, PUBLISH_TO_QMS, PUBLISH_TO_FILE };

/**
 * Exception thrown for an error in QMP processing.
 */
class QmpException : public QRException {
 public:
  /**
   * Creates an exception with text consisting of the passed template filled in
   * with the values of the other arguments.
   *
   * @param[in] msgTemplate Template for construction of the full message;
   *                        contains printf-style placeholders for arguments,
   *                        passed as part of a variable argument list.
   * @param[in] ... Variable argument list, consisting of a value for each
   *                placeholder in the message template.
   */
  QmpException(const char *msgTemplate...) : QRException() { qrBuildMessage(msgTemplate, msgBuffer_); }

  virtual ~QmpException() {}

};  // QmpException

class QmpPublish {
 public:
  QmpPublish(CollHeap *heap)
      : sqlInterface_(heap), target_(PUBLISH_TO_QMM), targetName_("QMM"), outFile_(NULL), server_(NULL), heap_(heap) {}

  virtual ~QmpPublish() {
    if (outFile_) {
      outFile_->close();
      delete outFile_;
    }
  }

  IpcEnvironment *getIpcEnvironment() const { return ipcEnv_; }

  void setIpcEnvironment(IpcEnvironment *env) { ipcEnv_ = env; }

  IpcServer *getServer() const { return server_; }

  void setServer(IpcServer *server) { server_ = server; }

  NABoolean setTarget(PublishTarget target, const char *targetFilename);

  /**
   * process reading from the PUBLISH_REWRITE table using a stream delete
   */
  void performRewritePublishReading();

 protected:
  /**
   * Prepare the row returned from the REWRITE_PUBLISH table
   */
  void preparePublishRewritePublishRowToSend(MVQR_Publish *publish);

  QRPublishDescriptorPtr createPublishDescriptor(MVQR_Publish *publish, NAString *mvDescriptorText);

  /**
   * convert the operation type literal to its enum equivalent
   */
  ComPublishMVOperationType convertOperationType(char *operation);

  /**
   * getNAHeap() obtains the NAHeap address to the XMLParser heap
   * @return The NAHeap heap address.
   */
  inline NAHeap *getNAHeap() { return (NAHeap *)heap_; };

 private:
  CollHeap *heap_;
  IpcEnvironment *ipcEnv_;
  PublishTarget target_;
  NAString targetName_;
  ofstream *outFile_;
  IpcServer *server_;
  QRQueriesImpl sqlInterface_;
};

#endif  // _QMPPUBLISH_H_
