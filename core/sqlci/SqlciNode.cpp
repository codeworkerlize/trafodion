#include "sqlci/SqlciNode.h"

#include <iostream>

SqlciNode::SqlciNode(const sqlci_node_type node_type_) : node_type(node_type_), next(nullptr), errcode(0) {
  strncpy(eye_catcher, "CI  ", 4);
};

SqlciNode::~SqlciNode(){};

short SqlciNode::process(SqlciEnv *sqlci_env) {
  std::cerr << "Error: virtual function process must be redefined. \n";
  return -1;
};
