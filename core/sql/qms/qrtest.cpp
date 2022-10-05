// **********************************************************************

// **********************************************************************

#include <stdio.h>
#include "qmscommon/XMLUtil.h"
#include "qmscommon/QRDescriptor.h"
#include "qmscommon/QRLogger.h"

/**
 * \file
 * A simple test application for serializing and deserializing XML documents
 * containing Query Rewrite descriptors. The test file, which contains the XML
 * for a descriptor, is hard-coded into the source. When the program is run, the
 * document is parsed, creating the corresponding hierarchy of element instances.
 * The XML document is then regenerated from those instances.
 */

// Static logger instance used within this file.
static QRLogger &logger = QRLogger::instance();

void output(char *text) { printf("%s\n", text); }

void testXML() {
  char inbuf[10000];
  XMLFormattedString outbuf;
  char *ptr = inbuf;
  // char fileName[] = "w:\\qms\\qrydesc.xml";
  // char fileName[] = "w:\\qms\\mvdesc.xml";
  char fileName[] = "w:\\qms\\resdesc.xml";
  FILE *fp = fopen(fileName, "r");
  if (!fp) {
    output("Could not open xml file for input");
    return;
  } else
    logger.log("Using input file %s", fileName);

  size_t len = fread(inbuf, 1, sizeof(inbuf), fp);
  if (len >= sizeof(inbuf)) output("Buffer overflow reading xml file");

  NAHeap xmlParseHeap("XML Parse Heap", NAMemory::DERIVED_FROM_SYS_HEAP, (int)32768);

  try {
    QRElementMapper em;
    XMLDocument doc = XMLDocument(&xmlParseHeap, em);
    XMLElementPtr descriptor = doc.parse(inbuf, (int)len, 1);
    if (!descriptor) {
      output("XMLDocument.parse() returned NULL");
    } else {
      descriptor->toXML(outbuf);
      output("Regurgitated xml:");
      output((char *)outbuf.data());
      FILE *outfp = fopen("w:\\qms\\desc_out.xml", "w");
      fwrite(outbuf.data(), 1, outbuf.length(), outfp);
    }
    deletePtr(descriptor);
  } catch (XMLException &ex) {
    output(ex.getMessage());
  } catch (QRDescriptorException &ex) {
    output(ex.getMessage());
  } catch (...) {
    output("Unknown exception occurred");
  }
}

int main() {
  testXML();
  return 0;
}
