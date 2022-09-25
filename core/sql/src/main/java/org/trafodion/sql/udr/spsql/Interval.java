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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Date and time interval
 */
public class Interval {
  private static final Logger LOG = Logger.getLogger(Interval.class);
  int days = 0;
  int microseconds = 0;
  
  /**
   * Add or subtract interval value to the specified date
   */
  public Date dateChange(Date in, boolean add) {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(in.getTime());
    calendarChange(c, add);
    return new Date(c.getTimeInMillis());
  }
  
  /**
   * Add or subtract interval value to the specified timestamp
   */
  public Timestamp timestampChange(Timestamp in, boolean add) {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(in.getTime());
    calendarChange(c, add);
    Timestamp out = new Timestamp(c.getTimeInMillis());
    // Calendar can only handle millis, we need to handle microseconds
    // manually
    if (add) {
      int nanos = in.getNanos() + (microseconds % 1000) * 1000;
      if (nanos >= 1000000000) {
        out.setTime(out.getTime() + 1);
        nanos -= 1000000000;
      }
      out.setNanos(nanos);
    } else {
      int nanos = in.getNanos() - (microseconds % 1000) * 1000;
      if (nanos < 0) {
        out.setTime(out.getTime() - 1);
        nanos += 1000000000;
      }
      out.setNanos(nanos);
    }
    return out;
  }
  
  /**
   * Add or subtract interval value to the specified time
   */
  public Time timeChange(Time in, boolean add) {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(in.getTime());
    calendarChange(c, add);
    Time out = new Time(c.getTimeInMillis());
    // Calendar can only handle millis, we need to handle microseconds
    // manually
    if (add) {
      int nanos = in.getNanos() + (microseconds % 1000) * 1000;
      if (nanos >= 1000000000) {
        out.setTime(out.getTime() + 1);
        nanos -= 1000000000;
      }
      out.setNanos(nanos);
    } else {
      int nanos = in.getNanos() - (microseconds % 1000) * 1000;
      if (nanos < 0) {
        out.setTime(out.getTime() - 1);
        nanos += 1000000000;
      }
      out.setNanos(nanos);
    }
    return out;
  }


  /**
   * Add interval value to the specified Calendar value
   */
  public Calendar calendarChange(Calendar c, boolean add) {
    int a = 1;
    if (!add) {
      a = -1;
    }
    if (days != 0) {
      c.add(Calendar.DAY_OF_MONTH, days * a);
    }
    if (microseconds != 0) {
      c.setTimeInMillis(c.getTimeInMillis() + (microseconds / 1000) * a);
    }
    return c;
  }
  
  /**
   * Set interval value
   */
  public Interval set(int value, String item) {
    if (item.compareToIgnoreCase("DAYS") == 0 || item.compareToIgnoreCase("DAY") == 0) {
      setDays(value);
    }
    if (item.compareToIgnoreCase("MICROSECONDS") == 0 || item.compareToIgnoreCase("MICROSECOND") == 0) {
      setMicroseconds(value);
    }
    return this;
  }
  
  /**
   * Set interval items
   */
  public void setDays(int days) {
    this.days = days;
  }
  
  public void setMicroseconds(int microseconds) {
    this.microseconds = microseconds;
  }
  
  public int getYear() {
    throw new IllegalArgumentException();
  }

  public int getMonth() {
    throw new IllegalArgumentException();
  }

  public int getDay() {
    return days;
  }

  public int getHour() {
    throw new IllegalArgumentException();
  }

  public int getMinute() {
    throw new IllegalArgumentException();
  }

  public int getSecond() {
    throw new IllegalArgumentException();
  }

  /**
   * Convert interval to string
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    if (days != 0) {
      s.append(days);
      s.append(" days");
    }
    if (microseconds != 0) {
      s.append(microseconds);
      s.append(" microseconds");
    }
    return s.toString();
  }
}
