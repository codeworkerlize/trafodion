/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trafodion.sql.udr.spsql;

import java.sql.Timestamp;

public class Time {
  private Timestamp ts;

  private Time(Timestamp ts) {
    this.ts = ts;
  }

  public Time(long millis) {
    this.ts = new Timestamp(millis);
  }

  public static Time valueOf(String s) {
    try {
      return new Time(Timestamp.valueOf("1111-01-01 " + s));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Time format must be hh:mm:ss[.fffffffff]");
    }
  }

  public String toString() {
    return ts.toString().substring(11);
  }

  public void setTime(long millis) {
    ts.setTime(millis);
  }

  public long getTime() {
    return ts.getTime();
  }

  public int getNanos() {
    return ts.getNanos();
  }

  public void setNanos(int nanos) {
    ts.setNanos(nanos);
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof Time) {
      Time t = (Time) obj;
      if (t.getTime() == getTime()
          && t.getNanos() == getNanos()) {
        return true;
      }
    }
    return false;
  }
}
