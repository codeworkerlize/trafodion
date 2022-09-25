/**
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**/


package org.apache.hadoop.hbase.client.transactional;


 // Transaction states
public enum TransState {
		STATE_NOTX(0), //S0 - NOTX
		STATE_ACTIVE(1), //S1 - ACTIVE
		STATE_FORGOTTEN(2), //N/A
		STATE_COMMITTED(3), //N/A
		STATE_ABORTING(4), //S4 - ROLLBACK
		STATE_ABORTED(5), //S4 - ROLLBACK
		STATE_COMMITTING(6), //S3 - PREPARED
		STATE_PREPARING(7), //S2 - IDLE
		STATE_FORGETTING(8), //N/A
		STATE_PREPARED(9), //S3 - PREPARED XARM Branches only!
		STATE_FORGETTING_HEUR(10), //S5 - HEURISTIC
		STATE_BEGINNING(11), //S1 - ACTIVE
		STATE_HUNGCOMMITTED(12), //N/A
		STATE_HUNGABORTED(13), //S4 - ROLLBACK
		STATE_IDLE(14), //S2 - IDLE XARM Branches only!
		STATE_FORGOTTEN_HEUR(15), //S5 - HEURISTIC - Waiting Superior TM xa_forget request
		STATE_ABORTING_PART2(16), // Internal State
		STATE_TERMINATING(17),
		STATE_FORGOTTEN_COMMITTED(18), //N/A
		STATE_FORGOTTEN_ABORT(19), //N/A
		STATE_RECOVERY_COMMITTED(20), //N/A
		STATE_RECOVERY_ABORT(21), //N/A
		STATE_LAST(21),
		STATE_BAD(-1);
    private Integer value;
    
    private TransState(int value) { this.value = new Integer(value); }
    private TransState(short value) { this.value = new Integer(value); }
    public short getShort() { return value.shortValue(); }
    public int getValue() { return value.intValue(); }
    public String toString() {
      return super.toString();
    }
}
