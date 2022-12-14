// **********************************************************************

// **********************************************************************

#include <HdfsLogger.h>
#include <log4cxx/configurator.h>
#include <log4cxx/patternlayout.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/rollingfileappender.h>

const char CAT_JNI_TOP[] = "JniTop";
const char CAT_SEQ_FILE_READER[] = "SeqFileReader";
const char CAT_SEQ_FILE_WRITER[] = "SeqFileWriter";
const char CAT_HBASE[] = "HBase";

// **************************************************************************
// **************************************************************************
HdfsLogger::HdfsLogger() : CommonLogger() {}

// **************************************************************************
// **************************************************************************
HdfsLogger &HdfsLogger::instance() {
  static HdfsLogger onlyInstance_;
  return onlyInstance_;
}

// **************************************************************************
// Call the superclass to configure log4cxx from the config file.
// If the configuration file is not found, perform default initialization:
// Create an appender, layout, and categories.
// Attaches layout to the appender and appender to categories.
// **************************************************************************
NABoolean HdfsLogger::initLog4cxx(const char *configFileName) {
  if (CommonLogger::initLog4cxx(configFileName)) return TRUE;

  NAString logFileName;

  // get the log directory
  logFileName = logFolder_;
  logFileName += "Hdfs.log";

  fileAppender_ = new log4cxx::RollingFileAppender("FileAppender", logFileName.data());

  log4cxx::PatternLayout *fileLayout = new log4cxx::PatternLayout();
  fileLayout->setConversionPattern("%d, %p, %c, %m%n");
  fileAppender_->setLayout(fileLayout);

  // Top level categories
  initCategory(CAT_SEQ_FILE_READER, log4cxx::Priority::ERROR);
  initCategory(CAT_SEQ_FILE_WRITER, log4cxx::Priority::ERROR);
  initCategory(CAT_HBASE, log4cxx::Priority::ERROR);

  // log4cxx::Category::getInstance(CAT_SEQ_FILE_READER).error("Failed to read the config file, using ERROR as logging
  // level.");
  return FALSE;
}

// **************************************************************************
// **************************************************************************
void HdfsLogger::initCategory(const char *cat, log4cxx::Priority::PriorityLevel defaultPriority) {
  log4cxx::Category &catObj = log4cxx::Category::getInstance(cat);
  catObj.setAppender(fileAppender_);
  catObj.setPriority(defaultPriority);
}
