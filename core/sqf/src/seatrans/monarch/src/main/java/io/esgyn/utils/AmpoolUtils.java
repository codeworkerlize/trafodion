package io.esgyn.utils;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.MServerLocation;
import io.ampool.monarch.table.internal.MTableLocationInfo;
import io.ampool.monarch.table.exceptions.*;

import io.esgyn.coprocessor.EsgynMGet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AmpoolUtils {

    static final Logger LOG = LogManager.getLogger(AmpoolUtils.class.getName());

    // key=table name
    static Map<String, List<MServerLocation>> sv_tli = new ConcurrentHashMap<String, List<MServerLocation>>();

    static public void printBytes(String pv_str) 
    {
	byte[] lv_ba = pv_str.getBytes();

	AmpoolUtils.printBytes(lv_ba);
    }

    static public void printBytes(byte[] pv_ba) 
    {

	System.out.print("[");
	for (byte lv_byte : pv_ba) {
	    System.out.print(lv_byte + " ");
	}

	System.out.println("]");
    }

    static public void printMTableLocation(MTable pv_mtable)
    {
	MTableLocationInfo lv_mtl = pv_mtable.getMTableLocationInfo();

	if (lv_mtl == null) {
	    return;
	}

	List<MServerLocation> lv_server_locations = lv_mtl.getAllMTableLocations();
	System.out.println("Number of mtable buckets: " + lv_server_locations.size());
	ListIterator<MServerLocation> lv_li_server_locations;
	int lv_bucket_index = 0;
	for (lv_li_server_locations = lv_server_locations.listIterator();
	     lv_li_server_locations.hasNext();) {
	    MServerLocation lv_server_location = lv_li_server_locations.next();
	    if (lv_server_location != null) {
		System.out.println("bucket index: " + lv_bucket_index
				   + ", server location: " + lv_server_location 
				   + ", server hash code: " + lv_server_location.hashCode()
				   );
	    }
	    lv_bucket_index++;
	}

    }

    // returns -1 if an error
    static public int getBucketId(MTable pv_mtable, byte[] pv_row_key)
    {
	if (LOG.isTraceEnabled()) LOG.trace("getBucketId()" 
					    + ", table name: " + pv_mtable.getName()
					    + ", row key: " + new String(pv_row_key)
					    );

	MTableLocationInfo lv_mtl = pv_mtable.getMTableLocationInfo();

	if (lv_mtl == null) {
	    return -1;
	}
	    
	MServerLocation lv_server_location = lv_mtl.getMTableLocation(pv_row_key);
	if (lv_server_location == null) {
	    return -1;
	}

	if (LOG.isTraceEnabled()) LOG.trace("server location: " + lv_server_location 
					    + ", server hash code: " + lv_server_location.hashCode()
					    );
	
	return lv_server_location.getBucketId();
    }

    static public MServerLocation getMServerLocation(MTable pv_mtable, byte[] pv_row_key)
    {
	if (LOG.isTraceEnabled()) LOG.trace("getMServerLocation()" 
					    + ", table name: " + pv_mtable.getName()
					    + ", row key: " + new String(pv_row_key)
					    );
	
	MTableLocationInfo lv_mtl = pv_mtable.getMTableLocationInfo();

	if (lv_mtl == null) {
	    return null;
	}
	    
	MServerLocation lv_server_location = lv_mtl.getMTableLocation(pv_row_key);

	if (LOG.isTraceEnabled()) LOG.trace("server location: " + lv_server_location
					    + ", server hash code: " 
					    + (lv_server_location != null ?
					       lv_server_location.hashCode():"null")
					    );
	
	return lv_server_location;
    }

    static public List<MServerLocation> getMServerLocationList(MTable pv_mtable)
    {
	List<MServerLocation> lv_server_locations = sv_tli.get(pv_mtable.getName());

	if (lv_server_locations == null) {
	    if (LOG.isDebugEnabled()) LOG.debug("getMServerLocationList"
						+ ", table: " + pv_mtable.getName()
						);
	    
	    MTableLocationInfo lv_mtl = pv_mtable.getMTableLocationInfo();
	    if (lv_mtl == null) {
		if (LOG.isDebugEnabled()) LOG.debug("getMServerLocationList - MTableLocationInfo is null"
						    + ", table: " + pv_mtable.getName()
						    );
		return null;
	    }
	
	    lv_server_locations = lv_mtl.getAllMTableLocations();
	    if (lv_server_locations != null) {
		sv_tli.put(pv_mtable.getName(), lv_server_locations);
	    }
	}
	
	return lv_server_locations;
    }

    static public int rowInLocation(byte[] pv_row_key,
				    MServerLocation pv_location)
    {
	if (pv_location == null) {
	    return -1;
	}

	int lv_cmp_end = io.ampool.monarch.table.Bytes.compareTo(pv_row_key,
								 pv_location.getEndKey()
								 );
	if (lv_cmp_end >= 0) {
	    return lv_cmp_end;
	}

	int lv_cmp_start = io.ampool.monarch.table.Bytes.compareTo(pv_row_key,
								   pv_location.getStartKey()
								   );

	if (lv_cmp_start >= 0) {
	    return 0;
	}

	return lv_cmp_start;
    }

    static public MServerLocation getLocationFromList(byte[] pv_row_key,
						      List<MServerLocation> pv_server_locations
						      )
    {
	if (LOG.isTraceEnabled()) LOG.trace("getLocationFromList" 
					    );
	
	ListIterator<MServerLocation> lv_li_server_locations;
	MServerLocation lv_server_location = null;

	for (lv_li_server_locations = pv_server_locations.listIterator();
	     lv_li_server_locations.hasNext();) {
	    lv_server_location = lv_li_server_locations.next();

	    if (lv_server_location == null) {
		continue;
	    }

	    if (rowInLocation(pv_row_key,
			      lv_server_location) == 0) {
		break;
	    }
	}

	if (LOG.isTraceEnabled()) LOG.trace("server location: " + lv_server_location
					    + ", server hash code: " 
					    + (lv_server_location != null ?
					       lv_server_location.hashCode():"null")
					    );
	
	return lv_server_location;
    }

    static public MServerLocation getMServerLocationX(MTable pv_mtable, byte[] pv_row_key)
    {
	if (LOG.isTraceEnabled()) LOG.trace("getMServerLocationX" 
					    + ", table name: " + pv_mtable.getName()
					    + ", row key: " + new String(pv_row_key)
					    );
	
	List<MServerLocation> lv_server_locations = getMServerLocationList(pv_mtable);

	if (lv_server_locations == null) {
	    return null;
	}
	    
	MServerLocation lv_server_location = getLocationFromList(pv_row_key,
								 lv_server_locations
								 );

	if (LOG.isTraceEnabled()) LOG.trace("server location: " + lv_server_location
					    + ", server hash code: " 
					    + (lv_server_location != null ?
					       lv_server_location.hashCode():"null")
					    );
	
	return lv_server_location;
    }

    static public byte[] getStartRowKey(MTable pv_mtable, int pv_bucket_id)
    {
	if (LOG.isTraceEnabled()) LOG.trace("getStartRowKey()" 
					    + ", table name: " + pv_mtable.getName()
					    + ", bucketId: " + pv_bucket_id
					    );
	
	MTableLocationInfo lv_mtl = pv_mtable.getMTableLocationInfo();

	if (lv_mtl == null) return null;
	
	List<MServerLocation> lv_server_locations = lv_mtl.getAllMTableLocations();

	ListIterator<MServerLocation> lv_li_server_locations;
	MServerLocation lv_server_location = null;

	for (lv_li_server_locations = lv_server_locations.listIterator();
	     lv_li_server_locations.hasNext();) {
	    lv_server_location = lv_li_server_locations.next();

	    if (lv_server_location == null) {
		continue;
	    }

	    if (lv_server_location.getBucketId() == pv_bucket_id) {
		break;
	    }

	}

	return lv_server_location.getStartKey();
    }

    static public Get prepare_mget(EsgynMGet p_get) 
    {

	Get lv_get = new Get(p_get.getRowKey());
	List<byte[]> lv_columns = p_get.getColumnNameList();

	int lv_num_cols = lv_columns.size();

	for (int lv_index = 0; lv_index < lv_num_cols; lv_index++) {
	    lv_get.addColumn(lv_columns.get(lv_index));
	}
	
	return lv_get;
    }

    static public Get prepare_mget_from_list(byte[]       pv_rk,
					      List<byte[]> pv_list) 
    {
	Get lv_get = new Get(pv_rk);

	ListIterator<byte[]> lv_li = null;

	for (lv_li = pv_list.listIterator();
	     lv_li.hasNext();
	     ) {
	
	    byte[] lv_curr = lv_li.next();
	    lv_get.addColumn(lv_curr);
	    
	}
	
	return lv_get;
    }

}
