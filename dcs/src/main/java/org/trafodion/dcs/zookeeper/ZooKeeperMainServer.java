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

package org.trafodion.dcs.zookeeper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeperMain;
import org.trafodion.dcs.util.DcsConfiguration;

/**
 * Tool for running ZookeeperMain from Dcs by  reading a ZooKeeper server
 * from DCS XML configuration.
 */
public class ZooKeeperMainServer {
  private static final String SERVER_ARG = "-server";

  public String parse(final Configuration c) {
    return ZKConfig.getZKQuorumServersString(c);
  }

  /**
   * @param args
   * @return True if argument strings have a '-server' in them.
   */
  private static boolean hasServer(final String args[]) {
    return args.length > 0 && args[0].equals(SERVER_ARG);
  }

  /**
   * @param args
   * @return True if command-line arguments were passed.
   */
  private static boolean hasCommandLineArguments(final String args[]) {
    if (hasServer(args)) {
      if (args.length < 2) throw new IllegalStateException("-server param but no value");
      return args.length > 2;
    }
    return args.length > 0;
  }

  /**
   * Run the tool.
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) throws Exception {
    String [] newArgs = args;
    if (!hasServer(args)) {
      // Add the zk ensemble from configuration if none passed on command-line.
      Configuration conf = DcsConfiguration.create();
      String hostport = new ZooKeeperMainServer().parse(conf);
      if (hostport != null && hostport.length() > 0) {
        newArgs = new String[args.length + 2];
        System.arraycopy(args, 0, newArgs, 2, args.length);
        newArgs[0] = "-server";
        newArgs[1] = hostport;
      }
      System.out.println((hostport == null || hostport.length() == 0)? "":
       "-server " + hostport);
    }
  }
}
