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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SysContext {
  private static final Logger LOG = Logger.getLogger(SysContext.class);

  private static final String DEFAULT_NAMESPACE = "userenv";
  private static final String PROPERTY_PREFIX = "sqlmx.udr.";
  Map<String, Map<String, String>> contextMap =
    new HashMap<String, Map<String, String>>();

  public void init() {
    put("username", "DB__ROOT");
    for (Map.Entry<Object, Object> prop : System.getProperties().entrySet()) {
      String propName = (String)prop.getKey();
      String propValue = (String)prop.getValue();
      if (propName.startsWith("sqlmx.udr.")) {
        String fullKey = propName.substring(PROPERTY_PREFIX.length());
        put(fullKey, propValue);
      }
    }
    dump();
  }

  void dump() {
    for (Map.Entry<String, Map<String, String>> entry: contextMap.entrySet()) {
      LOG.trace("SYS_CONTEXT NAMESPACE: " + entry.getKey());
      for (Map.Entry<String, String> prop :
             entry.getValue().entrySet()) {
        LOG.trace("    " + prop.getKey() + " = " + prop.getValue());
      }
    }
  }

  Map<String, String> getNS(String namespace) {
    return contextMap.get(namespace.toUpperCase());
  }

  public void put(String fullKey, String value) {
    ArrayList<String> parts = splitKey(fullKey);
    String namespace = parts.get(0);
    String key = parts.get(1);
    put(namespace, key, value);
  }

  public void put(String namespace, String key, String value) {
    Map<String, String> map = getNS(namespace);
    if (map == null) {
      map = new HashMap<String, String>();
      contextMap.put(namespace.toUpperCase(), map);
    }
    map.put(key.toUpperCase(), value);
  }

  public String get(String namespace, String key) {
    Map<String, String> map = getNS(namespace);
    if (map == null) {
      throw new IllegalArgumentException("SYS_CONTEXT namespace not found: "
                                         + namespace);
    }
    if (key.equalsIgnoreCase("session_user")) {
      key = "username";
    }
    String value = map.get(key.toUpperCase());
    if (value == null) {
      throw new IllegalArgumentException("SYS_CONTEXT namespace parameter not found: "
                                         + namespace + " " + key);
    }
    return value;
  }

  public String get(String namespace, String key, int len) {
    return Utils.substr(get(namespace, key), 0, len);
  }

  public String get(String fullKey) {
    ArrayList<String> parts = splitKey(fullKey);
    String namespace = parts.get(0);
    String key = parts.get(1);
    return getNS(namespace).get(key.toUpperCase());
  }

  ArrayList<String> splitKey(String fullKey) {
    ArrayList<String> parts = Meta.splitIdentifierToTwoParts(fullKey);
    if (parts == null) {
      parts = new ArrayList<String>();
      parts.add(DEFAULT_NAMESPACE);
      parts.add(fullKey);
    }
    return parts;
  }
}
