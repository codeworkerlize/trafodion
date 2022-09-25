// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.apache.hadoop.hbase.client.transactional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Multi Data Center specific
 */
public class PeerInfo {

    static final Log LOG = LogFactory.getLog(PeerInfo.class);

    public static final String TRAFODION_UP             = "tup";
    public static final String TRAFODION_DOWN           = "tdn";
    public static final String HBASE_UP                 = "hup";
    public static final String HBASE_DOWN               = "hdn";
    public static final String STR_UP                   = "sup";
    public static final String STR_DOWN                 = "sdn";

    /* For a checkAndPut() call by the client, do not do the check on the 
     * remote cluster.
     */
    public static final String PEER_ATTRIBUTE_NONE      = "a00"; 
    public static final String PEER_ATTRIBUTE_NO_CHECK  = "a01";
    public static final String PEER_ATTRIBUTE_SHIELD    = "a02";
    public static final String PEER_ATTRIBUTE_NO_CDC_XDC  = "a03";
    public static final String PEER_ATTRIBUTE_NO_CDC_IBR  = "b01";

    public enum PeerAttribute {
	PEER_ATTRIBUTE_NONE,
	PEER_ATTRIBUTE_NO_CHECK,
	PEER_ATTRIBUTE_SHIELD,
        PEER_ATTRIBUTE_NO_CDC_XDC,
        PEER_ATTRIBUTE_NO_CDC_IBR
	    };

    private String m_id;
    private String m_quorum;
    private String m_port;
    private boolean m_HBaseUp;
    private boolean m_TrafodionUp;
    private boolean m_STRUp;
    private boolean m_attr_no_check;
    private boolean m_attr_shield;
    private boolean m_attr_no_cdc_xdc;
    private boolean m_attr_no_cdc_ibr;
    
    private boolean m_complete_status;
    
    public PeerInfo() {
	if (LOG.isTraceEnabled()) LOG.trace("PeerInfo (s,s,s,s) -- ENTRY");

	m_complete_status = false;

	m_id = null;
	m_quorum = null;
	m_port = null;
	m_HBaseUp = false;
	m_TrafodionUp = false;
	m_STRUp = false;
	m_attr_no_check = false;
	m_attr_shield = false;
        m_attr_no_cdc_xdc = false;
        m_attr_no_cdc_ibr = false;
	}

    public PeerInfo(String pv_id, 
		    String pv_quorum, 
		    String pv_port, 
		    String pv_status,
		    String pv_peer_attribute) 
    {
	if (LOG.isTraceEnabled()) LOG.trace("PeerInfo (s,s,s,s) -- ENTRY");

	m_id = pv_id;
	m_quorum = pv_quorum;
	m_port = pv_port;
	m_HBaseUp = false;
	m_TrafodionUp = false;
	m_STRUp = false;
	set_internal_status_fields(pv_status);
	set_internal_status_fields(pv_peer_attribute);
    }

    public String get_id() {
	return m_id;
    }

    public void set_id(String pv_id) {
	m_id = pv_id;
    }

    public String get_quorum() {
	return m_quorum;
    }

    public void set_quorum(String pv_quorum) {
	m_quorum = pv_quorum;
    }

    public void set_quorum(byte[] pv_quorum) {
	if (pv_quorum != null) {
	    m_quorum = new String(pv_quorum);
	}
    }

    public String get_port() {
	return m_port;
    }

    public void set_port(String pv_port) {
	m_port = pv_port;
    }

    public void set_port(byte[] pv_port) {
	if (pv_port != null) {
	    m_port = new String(pv_port);
	}
    }

    public void set_complete_status(boolean pv_complete_status) {
	m_complete_status = pv_complete_status;
    }
    
    public void update_peer_attribute(String peer_attribute)
    {

      if (peer_attribute.contains(PEER_ATTRIBUTE_NO_CDC_IBR)) {
         boolean reset = peer_attribute.startsWith("-");
         m_attr_no_cdc_ibr = reset? false: true;
      }

      if (peer_attribute.contains(PEER_ATTRIBUTE_NONE)) {
        m_attr_no_check = false;
        m_attr_shield = false;
        m_attr_no_cdc_xdc = false;
      }
      else {
        boolean reset = peer_attribute.startsWith("-");
        
        if (peer_attribute.contains(PEER_ATTRIBUTE_NO_CHECK)) {
          m_attr_no_check = reset? false: true;
        }
        if (peer_attribute.contains(PEER_ATTRIBUTE_SHIELD)) {
          m_attr_shield = reset? false: true;
        }
        if (peer_attribute.contains(PEER_ATTRIBUTE_NO_CDC_XDC)) {
          m_attr_no_cdc_xdc = reset? false: true;
        }
      }

    }

    public String get_status() {
	StringBuilder lv_sb = new StringBuilder();
	
	get_status(lv_sb);

	return lv_sb.toString();
    }

    public String get_trafodion_status_string() {

	// Trafodion status
	if (m_TrafodionUp) {
	    return TRAFODION_UP;
	}
	
	return TRAFODION_DOWN;
    }

    public String  get_STR_status_string() {
	// STR Status
	if (m_STRUp) {
	    return STR_UP;
	}

	return STR_DOWN;

    }

    public boolean is_shield() {
        return m_attr_shield;
    }

    public String get_attribute_string() {
    
    StringBuilder lv_sb = new StringBuilder();
    boolean atleastOneAttr = false;
    
    // PEER Attribute
    if (m_attr_no_check) {
    lv_sb.append(PEER_ATTRIBUTE_NO_CHECK);
    atleastOneAttr = true;
    }
    
    if (m_attr_shield) {
    if(atleastOneAttr) lv_sb.append("-");
    lv_sb.append(PEER_ATTRIBUTE_SHIELD);
    atleastOneAttr = true;
    }

    if (m_attr_no_cdc_xdc) {
    if(atleastOneAttr) lv_sb.append("-");
    lv_sb.append(PEER_ATTRIBUTE_NO_CDC_XDC);
    atleastOneAttr = true;
    }
    
    if(!atleastOneAttr) {
    lv_sb.append(PEER_ATTRIBUTE_NONE);
    atleastOneAttr = true;
    }

    if (m_attr_no_cdc_ibr) {
       if(atleastOneAttr) lv_sb.append("-");
       lv_sb.append(PEER_ATTRIBUTE_NO_CDC_IBR);
       lv_sb.append("-no_cdc_IBR");
    }
    
    return lv_sb.toString();
	}


    public void get_status(StringBuilder pv_sb) {

	if (pv_sb == null) {
	    return;
	}

	// Trafodion status
	if (m_complete_status) {
	    if (m_TrafodionUp) {
		pv_sb.append(TRAFODION_UP);
	    }
	    else {
		pv_sb.append(TRAFODION_DOWN);
	    }
	    pv_sb.append("-");
	}

	// STR Status
	if (m_STRUp) {
	    pv_sb.append(STR_UP);
	}
	else {
	    pv_sb.append(STR_DOWN);
	}

	if (m_complete_status) {
	    
	    boolean atleastOneAttr = false;
	    // PEER Attribute
	    if (m_attr_no_check) {
	    pv_sb.append("-");
		pv_sb.append(PEER_ATTRIBUTE_NO_CHECK);
		atleastOneAttr = true;
	    }
	    
	    if (m_attr_shield) {
	    pv_sb.append("-");
	    pv_sb.append(PEER_ATTRIBUTE_SHIELD);
	    atleastOneAttr = true;
	    }
	    
            if (m_attr_no_cdc_xdc) {
                pv_sb.append("-");
                pv_sb.append(PEER_ATTRIBUTE_NO_CDC_XDC);
                atleastOneAttr = true;
            }
            
	    if(!atleastOneAttr) {
	        pv_sb.append("-");
		pv_sb.append(PEER_ATTRIBUTE_NONE);
                atleastOneAttr = true;
	    }


            if (m_attr_no_cdc_ibr) {
               if(atleastOneAttr) pv_sb.append("-");
               pv_sb.append(PEER_ATTRIBUTE_NO_CDC_IBR);
               pv_sb.append(":no_cdc_IBR");
            }

	}

    }

    private void set_internal_status_fields(String pv_status) {
	//HBase status
	if (pv_status.contains(HBASE_UP)) {
	    m_HBaseUp = true;
	}
	if (pv_status.contains(HBASE_DOWN)) {
	    m_HBaseUp = false;
	}

	//Trafodion status
	if (pv_status.contains(TRAFODION_UP)) {
	    m_TrafodionUp = true;
	}
	if (pv_status.contains(TRAFODION_DOWN)) {
	    m_TrafodionUp = false;
	}

	// STR status
	if (pv_status.contains(STR_UP)) {
	    m_STRUp = true;
	}
	if (pv_status.contains(STR_DOWN)) {
	    m_STRUp = false;
	}

	//PEER Attribute
	if (pv_status.contains(PEER_ATTRIBUTE_NONE)) {
	    m_attr_no_check = false;
	    m_attr_shield = false;
            m_attr_no_cdc_xdc = false;
	}
	if (pv_status.contains(PEER_ATTRIBUTE_NO_CHECK)) {
	    m_attr_no_check = true;
	}
	if (pv_status.contains(PEER_ATTRIBUTE_SHIELD)) {
	    m_attr_shield = true;
	}
        if (pv_status.contains(PEER_ATTRIBUTE_NO_CDC_XDC)) {
            m_attr_no_cdc_xdc = true;
        }
        if (pv_status.contains(PEER_ATTRIBUTE_NO_CDC_IBR)) {
            m_attr_no_cdc_ibr = true;
        }
        else {
            m_attr_no_cdc_ibr = false;
         }

    }

    public void set_status(String pv_status) {
	set_internal_status_fields(pv_status);
    }
	
    public void set_status(byte[] pv_status) {
	if (pv_status != null) {
	    String lv_status = new String(pv_status);
	    set_internal_status_fields(lv_status);
	}
    }

    public void setHBaseStatus(boolean pv_status) {
	m_HBaseUp = pv_status;
    }

    public void setTrafodionStatus(boolean pv_status) {
	m_TrafodionUp = pv_status;
    }

    public void setSTRStatus(boolean pv_status) {
	m_STRUp = pv_status;
    }

    public boolean isHBaseUp() {
	return m_HBaseUp;
    }

    public boolean isTrafodionUp() {
	return m_TrafodionUp;
    }

    public boolean isSTRUp() {
	return m_STRUp;
    }

    public String toString()  
    {
	StringBuilder lv_sb = new StringBuilder();
	lv_sb = lv_sb
	    .append(m_id)
	    .append(":")
	    .append(m_quorum)
	    .append(":")
	    .append(m_port)
	    .append(":");

	get_status(lv_sb);

	return lv_sb.toString();
    }

    public static void main(String [] Args) throws Exception 
    {
	PeerInfo lv_peer = new PeerInfo("1", "q", "24000", "sup", "a01");
	if (LOG.isTraceEnabled()) LOG.trace("PeerInfo peer " + lv_peer);
	System.exit(0);
    }

}
