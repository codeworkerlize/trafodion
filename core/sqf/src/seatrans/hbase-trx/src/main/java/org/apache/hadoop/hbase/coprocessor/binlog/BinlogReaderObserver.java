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

package org.apache.hadoop.hbase.coprocessor.binlog;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.transactional.ATRConfig;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.pit.HBaseBinlog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.zookeeper.KeeperException;

public class BinlogReaderObserver extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(BinlogReaderObserver.class);
    private Table binlogReaderTable;
    public static final String BINLOG_NAMESPACE = "TRAF_RSRVD_5";
    public final static String BINLOG_READER_TABLE_NAME = "TRAF_RSRVD_5:TRAFODION._DTM_.BINLOG_READER";
    public final static byte[] BINLOG_META_FAMILY = Bytes.toBytes("mt_");
    public final static String BINLOG_READER_COPROCESSOR_CLASS_NAME = "org.apache.hadoop.hbase.coprocessor.binlog.BinlogReaderObserver";

    @Override
    public void postPut (ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {
        if(ATRConfig.instance().isATRXDCEnabled() == false)
          return;
        if (ATRConfig.instance().isRemote()) {
            LOG.info("BinlogReaderObserver postPut start to update remote BINLOG_READER");
            try {
                binlogReaderTable = getBinlogReaderTable();
                binlogReaderTable.put(put);
            } catch (Exception ex) {
                LOG.warn("BinlogReaderObserver update remote BINLOG_READER failed due to exception:", ex);
            }
        }
    }

    private Table getBinlogReaderTable() throws IOException, KeeperException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        String[] hostAndPort = ATRConfig.instance().getBinlogConnectionString().split(":");
        conf.set("hbase.zookeeper.quorum", hostAndPort[0]);
        if (hostAndPort.length > 1) {
            conf.setInt("hbase.zookeeper.property.clientPort", Integer.parseInt(hostAndPort[1]));
        }
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean binlogReaderTableExists = admin.isTableAvailable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
        if (LOG.isTraceEnabled()) LOG.trace("binlog reader Table " + BINLOG_READER_TABLE_NAME + (binlogReaderTableExists? " exists" : " does not exist" ));

        if (binlogReaderTableExists == false) {
            try{
                NamespaceDescriptor nsDescriptor =  NamespaceDescriptor.create(BINLOG_NAMESPACE).build();
                admin.createNamespace(nsDescriptor);
            } catch(NamespaceExistException e) {
                // just ignore
            }

            HTableDescriptor mbindesc = new HTableDescriptor(TableName.valueOf(BINLOG_READER_TABLE_NAME));
            HColumnDescriptor mhcol = new HColumnDescriptor(BINLOG_META_FAMILY);
            mhcol.setMaxVersions(1000);
            mbindesc.addFamily(mhcol);

            try {
                if (LOG.isTraceEnabled()) LOG.trace("try new HTable: " + BINLOG_READER_TABLE_NAME);
                admin.createTable(mbindesc);
            } catch(TableExistsException tee){
                int retryCount = 0;
                boolean binlogReaderTableEnabled = false;
                while (retryCount < 3) {
                    retryCount++;
                    binlogReaderTableEnabled = admin.isTableAvailable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
                    if (! binlogReaderTableEnabled) {
                        try {
                            Thread.sleep(2000); // sleep two seconds or until interrupted
                        } catch (InterruptedException e) {
                            // Ignore the interruption and keep going
                        }
                    }
                    else {
                        break;
                    }
                }
                if (retryCount == 3){
                    LOG.error("BinlogReaderObserver Exception while creating remote " + BINLOG_READER_TABLE_NAME);
                    throw new IOException("BinlogReaderObserver Exception while creating remote " + BINLOG_READER_TABLE_NAME);
                }
            } catch (Exception e) {
                LOG.error("BinlogReaderObserver Exception while creating remote " + BINLOG_READER_TABLE_NAME + ": " , e);
                throw new IOException("BinlogReaderObserver Exception while creating remote " + BINLOG_READER_TABLE_NAME + ": " + e);
            }
        }
        return connection.getTable(TableName.valueOf(BINLOG_READER_TABLE_NAME));
    }
}
