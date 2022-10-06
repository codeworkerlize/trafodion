// **********************************************************************

// **********************************************************************

#ifndef _QRMESSAGE_H
#define _QRMESSAGE_H

#include "common/Ipc.h"

/**
 * \file
 * Contains various defines and constant definitions used by Query Rewrite
 * components and clients.
 */

namespace QR {
const char CURRENT_VERSION[] = "1.0";

/**
 * Enumeration of possible values for the \c result attribute.
 */
enum QRRequestResult {
  Success,
  InvalidRequest,  /**< Could not parse request header. */
  XMLParseError,   /**< Could not parse request body. */
  Unable,          /**< Could not provide a QMS. */
  Timeout,         /**< Response not received within time limits. */
  NotReady,        /**< QMS has not completed initialization. */
  WrongDescriptor, /**< Descriptor accompanying request does not match request type. */
  BadFile,         /**< Could not open specified XML file (command-line QMS only). */
  ProtocolError,   /**< Send/receive error using IPC. */
  InternalError    /**< Unexplained failure. */
};

// NOTE: The following enum must remain in sync with the definition of
//       QRMessageObj::MessageTypeNames, which contains the corresponding names.
/**
 * enum listing values used both as message types, and types of the
 * corresponding message objects.
 */
enum QRMessageTypeEnum {
  UNSPECIFIED_QR_MESSAGE = IPC_MSG_QR_FIRST,

  // Requests
  INITIALIZE_REQUEST,
  ALLOCATE_REQUEST,
  PUBLISH_REQUEST,
  MATCH_REQUEST,
  CHECK_REQUEST,
  CLEANUP_REQUEST,
  DEFAULTS_REQUEST,

  // These are used only for command-line QMS
  COMMENT_REQUEST,   // comment line in command file
  WORKLOAD_REQUEST,  // Perform workload analysis.
  ERROR_REQUEST,     // returned if invalid request name given in command file

  // Responses. All requests return a STATUS_RESPONSE on failure; PUBLISH,
  // UPDATE, INITIALIZE, and CLEANUP have nothing else to return, so return a
  // "success" status response when there is no failure.
  STATUS_RESPONSE,
  ALLOCATE_RESPONSE,
  MATCH_RESPONSE,
  CHECK_RESPONSE,
};

class QRMessage {
 public:
  static QRMessageTypeEnum resolveRequestName(char *name);
  static const char *getRequestName(QRMessageTypeEnum type);

  struct ReqNames {
    QRMessageTypeEnum type;
    char name[20];
  };
};

};     // namespace QR
#endif /* _QRMESSAGE_H */
