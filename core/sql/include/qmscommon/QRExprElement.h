// **********************************************************************

// **********************************************************************

#ifndef _QREXPRELEMENT_H_
#define _QREXPRELEMENT_H_

/**
 * \file
 * Contains the \c ExprElement enum, which is used in several optimizer header
 * files for the \c ItemExpr hierarchy. A change to those headers causes a long
 * compile, so this enum is placed in its own file instead of QRDescriptor.h,
 * thus avoiding a dependency on that file and a big recompilation whenever
 * it changes.
 */

// This is an extension of the QR namespace, some elements of which are also
// defined in QRMessage.h.
namespace QR {
/**
 * Enumeration representing element types that can appear as subelements in
 * an &lt;Expr&gt; element. This is used in a virtual function in \c ItemExpr
 * to indicate what element we should generate when a node of an item
 * expression is visited.
 */
enum ExprElement {
  QRNoElem,
  QRFunctionElem,
  QRFunctionWithParameters,
  QRBinaryOperElem,
  QRUnaryOperElem,
  QRColumnElem,
  QRScalarValueElem
};
};  // namespace QR

#endif /* _QREXPRELEMENT_H_ */
