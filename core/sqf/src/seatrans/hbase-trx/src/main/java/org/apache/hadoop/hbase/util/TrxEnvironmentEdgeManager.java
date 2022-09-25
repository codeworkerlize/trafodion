/**
 * * @@@ START COPYRIGHT @@@
 * *
 * * Licensed to the Apache Software Foundation (ASF) under one
 * * or more contributor license agreements.  See the NOTICE file
 * * distributed with this work for additional information
 * * regarding copyright ownership.  The ASF licenses this file
 * * to you under the Apache License, Version 2.0 (the
 * * "License"); you may not use this file except in compliance
 * * with the License.  You may obtain a copy of the License at
 * *
 * *   http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing,
 * * software distributed under the License is distributed on an
 * * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * * KIND, either express or implied.  See the License for the
 * * specific language governing permissions and limitations
 * * under the License.
 * *
 * * @@@ END COPYRIGHT @@@
 * **/
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class TrxEnvironmentEdgeManager {
  private static final Log LOG = LogFactory.getLog(TrxEnvironmentEdgeManager.class);
  private static long maxTime = 0L;

  public static EnvironmentEdge getDelegate() {
    return EnvironmentEdgeManager.getDelegate();
  }

  public static void reset() {
    EnvironmentEdgeManager.reset();
  }

  public static void injectEdge(EnvironmentEdge edge) {
    EnvironmentEdgeManager.injectEdge(edge);
  }

  /**
   * Defers to the delegate and calls the
   * {@link EnvironmentEdgeManager#currentTime()} method.
   *
   * @return current time in millis according to the delegate.
   */
  public static synchronized long currentTime() {
    long curTime = EnvironmentEdgeManager.currentTime();

    while (maxTime > curTime) {
      LOG.warn("Timestamp returned from EnvironmentEdgeManager's currentTime function does not monotonically increase");
      try {
        Thread.sleep(maxTime - curTime + 1);
      } catch (InterruptedException ie) {
      }
      curTime = EnvironmentEdgeManager.currentTime();
    }
    maxTime = curTime;
    return maxTime;
  }
}
