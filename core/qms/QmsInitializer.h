// **********************************************************************

// **********************************************************************

#ifndef _QMSINITIALIZE_H_
#define _QMSINITIALIZE_H_

#include <fstream>

#include "QRQueriesImpl.h"
#include "QmsQms.h"
#include "common/NAString.h"
#include "qmscommon/QRDescriptor.h"
#include "qmscommon/QRLogger.h"

class QmsInitializer {
 public:
  static QmsInitializer *getInstance(Qms *qms = NULL) {
    if (!instance_) instance_ = new (qms->getHeap()) QmsInitializer(qms->getHeap(), qms);
    return instance_;
  }

  virtual ~QmsInitializer() {}

  /**
   * performInitialization performs the QMS initialization
   * to obtain all MV descriptor text and MV attributes
   * into memory for use by the QMS process.
   * @return The return code from internal SQL query execution.
   */
  int performInitialization();

  /**
   * doCollectQMSStats checks whether we should collect qms stats
   * @return The flag whether stats should be collected
   */
  inline NABoolean doCollectQMSStats() { return collectQMSStats_; };

  /**
   * getCatalogNumber returns the number of catalogs
   * successfully processed that contained MVs.
   * @return The number of catalogs processed containing MVs.
   */
  inline int getNumberOfCatalogs() { return numberOfCatalogs_; };

  /**
   * getMVNumber returns the number of MV
   * successfully processed that contained valid MV descriptors.
   * @return The number of MVs processed containing valid descriptors
   */
  inline int getNumberOfMVs() { return numberOfMVs_; };

  /**
   * dump QMS statistics to the log
   * statistics collection and logging is controlled
   * by default MVQR_COLLECT_QMS_STATS
   */
  void logQMSStats();

  /**
   * For each and ever MV defined, call catman to regenerate the
   * MV descriptor.
   */
  void reDescriber(NAString *mvName, NABoolean rePublish);

  Qms *getQms() const { return qms_; }

  CollHeap *getHeap() const { return heap_; }

 protected:
  QmsInitializer(CollHeap *heap, Qms *qms)
      : heap_(heap), qms_(qms), sqlInterface_(heap), numberOfCatalogs_(0), numberOfMVs_(0), collectQMSStats_(FALSE) {
    isoMapping_ = CharInfo::getCharSetEnum((const char *)"ISO88591");
    defaultCharset_ = CharInfo::getCharSetEnum((const char *)"ISO88591");
  }

  /**
   * processMvqrPrivateQMSInit will determine if
   * MVQR_PRIVATE_QMS_INIT has been set either via
   * a CQD or through the system defaults table.
   */
  void processMvqrPrivateQMSInit();

  /**
   * processCharset obtains the ISO_MAPPING and DEFAULT_CHARSET
   * attribute values from the system defaults table
   */
  void processCharset();

  /**
   * processCatalogs will process all the catalog names
   * on the system to obtain all the MVs, MV descriptors,
   * and MV attributes
   */
  void processCatalogs(const NAStringList *catalogNames);

  /**
   * processMvDescriptor will process all the MV UIDs
   * found on the system to obtain all the MV descriptors
   * and MV attributes
   */
  void processMvDescriptor(const QRMVDefinition *mvDef, const NAString *mvDescriptorText);

  /**
   * Collect the names of all the MVs in the input list of catalogs
   * and add them to mvNames.
   * @param catalogNames [IN] list of catalog names.
   * @param mvNames [OUT] list of MV names in all the catalogs.
   * @return TRUE for success, FALSE for failure.
   */
  NABoolean collectMVNames(const NAStringList *catalogNames, NAStringList &mvNames);

  /**
   * For each MV in the list of MV names, call catman to regenerate
   * the MV descriptor.
   * @param mvNames [IN] list of all MV names.
   */
  void reDescribeMVs(NAStringList &mvNames, NABoolean rePublish);

 private:
  static QmsInitializer *instance_;
  CollHeap *heap_;
  QRQueriesImpl sqlInterface_;
  Qms *qms_;
  NABoolean collectQMSStats_;
  CollIndex numberOfCatalogs_;
  CollIndex numberOfMVs_;
  CharInfo::CharSet isoMapping_;
  CharInfo::CharSet defaultCharset_;
};

#endif /* _QMSINITIALIZE_H_ */
