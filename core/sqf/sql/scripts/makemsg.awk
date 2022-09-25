#
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#

# This version handles message texts that can cross multiple text lines.
# A backslash character at the end of the line indicates the 
# the continuation to the following line.

# Variable usage description: 
#                 msg_id = the id of the message. 
#                 mode = the mode of operation. Valid mode = 1 or 2.
#                        Mode 1 indicates normal mode while mode 2 indicates
#                        we are in the text continuation mode.
#
BEGIN   { prev_set = -1; msg_id = ""; mode = 1; rec_str = "" }
END { print rec_str }
NF == 0 { next }

{ 
  if ( substr($0, 1, 6) == "[[SQL-" ) {
    if (length(rec_str) > 0){
      print rec_str "\n"
      rec_str = ""
    }

    msg_id = substr($0, 7, index($0, "]]") - 7)
    current_set = int(msg_id / 10) * 10
    
    if (current_set != prev_set)
    {
      prev_set = current_set
      print "$set " current_set
    }
    mode = 1
  }
  else if (mode == 1) {
    mode++
  }
  else if ( substr($0, 1, 8) == "*Cause:*" ) {
    mode = 4
    rec_str = rec_str "\nC_" msg_id substr($0, 9)
  }
  else if ( substr($0, 1, 9) == "*Effect:*" ) {
    mode = 5
    rec_str = rec_str "\nF_" msg_id substr($0, 10)
  }
  else if ( substr($0, 1, 11) == "*Recovery:*" ) {
    mode = 6
    rec_str = rec_str "\nR_" msg_id substr($0, 12)
  }
  else {
    if ( mode == 2 ) {
      mode++
      rec_str = "E_" msg_id " " $0
    }
    else {
      rec_str = rec_str "\\n\\\n" $0
    }
  }
}
