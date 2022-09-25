/**
 * Copyright The Apache Software Foundation
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
package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;

import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AmpoolRegionInfo {
    static final Logger LOG = LogManager.getLogger(AmpoolRegionInfo.class.getName());

    MTable m_table ;
    MTableRegion m_tableregion;
    String m_region_name = null;
    TableName m_table_name = null;

    private AmpoolRegionInfo() {}

    public AmpoolRegionInfo(MTable p_table,
			    MTableRegion p_tableregion) {
	m_table = p_table;
	m_tableregion = p_tableregion;

    }
    
    public String getEncodedName() {

	if (m_region_name == null) {
	
	    StringBuilder lv_return = new StringBuilder();
	
	    lv_return.append(m_table.getName());
	    lv_return.append("#se#");
	    lv_return.append(new String(m_tableregion.getStartKey()));
	    lv_return.append("#se#");
	    lv_return.append(new String(m_tableregion.getEndKey()));
	
	    m_region_name = lv_return.toString();
	    if (LOG.isDebugEnabled()) LOG.debug("AmpoolRegionInfo: getEncodedName constructed: " + m_region_name);
	}

	if (LOG.isDebugEnabled()) LOG.debug("AmpoolRegionInfo: getEncodedName(): " + m_region_name);
	return m_region_name;
	
    }

    public byte[] getEncodedNameAsBytes() {
	return getEncodedName().getBytes();
    }

    public String getRegionNameAsString() {
	if (LOG.isDebugEnabled()) LOG.debug("AmpoolRegionInfo: getRegionNameAsString, table name: " + m_table.getName());
	return getEncodedName();
    }

    public long getRegionId() {
	return 100;
    }

    public MTable getMTable() {
	return m_table;
    }

  /**
   * Gets the table name from the specified region name.
   * Like {@link #getTableName(byte[])} only returns a {@link TableName} rather than a byte array.
   * @param regionName
   * @return Table name
   * @see #getTableName(byte[])
   */
  public static TableName getTable(final byte [] pv_region_name) {
      return TableName.valueOf(pv_region_name);
  }

  /**
   * Get current table name of the region
   * @return TableName
   * @see #getTableName()
   */
    public TableName getTable() {
	// This method name should be getTableName but there was already a method getTableName
	// that returned a byte array.  It is unfortunate given everwhere else, getTableName returns
	// a TableName instance.
	if (m_table_name == null || m_table_name.getName().length == 0) {
	    //	    m_table_name = getTable(getEncodedNameAsBytes());
	    m_table_name = getTable(m_table.getName().getBytes());
	}
	return this.m_table_name;
    }

    public String toString() {
	return getEncodedName();
    }
}
