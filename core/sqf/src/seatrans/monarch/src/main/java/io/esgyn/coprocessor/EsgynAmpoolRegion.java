package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EsgynAmpoolRegion {
    static final Logger LOG = LogManager.getLogger(EsgynAmpoolRegion.class.getName());
    
    public MTableRegion     m_mtableregion = null;
    public AmpoolRegionInfo m_region_info = null;
    public AtomicLong       m_sequence_id = null;

    public EsgynAmpoolRegion(MTableRegion p_mtableregion) 
    {
	m_mtableregion = p_mtableregion;
    }

    private EsgynAmpoolRegion() {}

    public MTableDescriptor getTableDesc() {
	if (m_mtableregion == null) {
	    return null;
	}

	return m_mtableregion.getTableDescriptor();
    }
    
    public void setRegionInfo(AmpoolRegionInfo pv_region_info) {
	if (LOG.isDebugEnabled()) LOG.debug("EsgynAmpoolRegion: setRegionInfo: " + pv_region_info);
	m_region_info = pv_region_info;
    }

    public AmpoolRegionInfo getRegionInfo() {
	if (LOG.isDebugEnabled()) LOG.debug("EsgynAmpoolRegion: getRegionInfo, returning: " + m_region_info);
	return m_region_info;
    }

    public AtomicLong getSequenceId() {
	return m_sequence_id;
    }

    void flushcache() {
	return;
    }

    void put(Put pv_put) {
	m_mtableregion.put(pv_put);
    }

    void delete(Delete pv_delete) {
	m_mtableregion.delete(pv_delete);
    }

}
