/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */

package com.esgyn.common.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Adds rest configuration files to a Configuration
 */
public class TrafodionSiteConfiguration extends Configuration {

  public static Configuration addTrafodionSiteResources(Configuration conf) {
      String traf_conf = System.getenv("TRAF_CONF");
      if(traf_conf != null && traf_conf.length() > 0) {
          conf.addResource(new Path(traf_conf + "/trafodion-site.xml"));
      }
    return conf;
  }

  /**
   * Creates a Configuration with Trafodion site resources
   * @return a Configuration with Trafodion site resources
   */
  public static Configuration create() {
    Configuration conf = new Configuration(false);
    return addTrafodionSiteResources(conf);
  }

  /**
   * @param that Configuration to clone.
   * @return a Configuration created with the trafodion-site.xml files plus
   * the given configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

  /**
   * Merge two configurations.
   * @param destConf the configuration that will be overwritten with items
   *                 from the srcConf
   * @param srcConf the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }
  
  public static void save(Configuration conf) throws IOException {
      String traf_conf = System.getenv("TRAF_CONF");
      if(traf_conf != null && traf_conf.length() > 0) {
          FileWriter writer = new FileWriter(traf_conf + "/trafodion-site.xml");
          conf.writeXml(writer);
          writer.close();
      }
  }
  
  /** For debugging.  Dump configurations to system output as xml format.
   * Master and RS configurations can also be dumped using
   * http services. e.g. "curl http://master:60010/dump"
   */
  public static void main(String[] args) throws Exception {
    TrafodionSiteConfiguration.create().writeXml(System.out);
  }
}
