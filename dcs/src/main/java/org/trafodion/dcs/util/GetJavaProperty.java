/**
/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.trafodion.dcs.util;

import org.trafodion.dcs.Constants;

/**
 * A generic way for querying Java properties.
 */
public class GetJavaProperty {
  public static void main(String args[]) {
    if (args.length == 0) {
      for (Object prop: System.getProperties().keySet()) {
        System.out.println(prop + "=" + System.getProperty((String)prop, ""));
      }
    } else {
      for (String prop: args) {
        System.out.println(System.getProperty(prop, ""));
      }
    }
  }

    private static String dcsHome = null;
    public static String getProperty(String key) {
        return System.getProperty(key, "");
    }

    public static void setProperty(String key, String value) {
        System.setProperty(key, value);
    }

    public static String getEnv(String key) {
        return System.getenv(key) != null ? System.getenv(key) : "";
    }

    public static String getDcsHome() {
        if (dcsHome != null) {
            return dcsHome;
        } else {
            String tmpHome = getProperty(Constants.DCS_HOME_DIR);
            String tmpHome2 = getEnv(Constants.DCS_INSTALL_DIR);
            return tmpHome.length() != 0 ? tmpHome : tmpHome2;
        }
    }
}
