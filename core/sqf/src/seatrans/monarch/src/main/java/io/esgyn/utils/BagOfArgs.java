package io.esgyn.utils;

import io.esgyn.coprocessor.*;
import io.esgyn.utils.*;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.esgyn.ipc.AmpoolIPC;
import io.esgyn.utils.AmpoolUtils;

public class BagOfArgs {

    String m_table_name    = null;
    int  m_bucket_id       = -1;
    long m_xid             = 100;
    String m_rk            = null;
    byte[] m_rk_bytes      = null;
    String m_locator_host  = "localhost";
    int m_locator_port     = 25510;
    boolean m_verbose      = false;
    String m_entered_key   = "99900";
    String m_log_file_name = null;
    boolean m_load         = false;
    String  m_prefix       = null;
    long m_num_entries     = 1000;
    boolean m_validate     = true;
    
    public BagOfArgs() {
    }

    public BagOfArgs(String pv_log_file_name) {
	m_log_file_name = pv_log_file_name;
    }

    public BagOfArgs(String pv_locator_host, int pv_locator_port, String pv_log_file_name) {
	m_locator_host = pv_locator_host;
	m_locator_port = pv_locator_port;
	m_log_file_name = pv_log_file_name;
    }

    public void setValidate(boolean pv_validate) {
	m_validate = pv_validate;
    }

    public int parseArgs(String args[]) {

	int lv_curr_arg = 0;

	while (lv_curr_arg < args.length) {
	    if (m_verbose) System.out.println("Curr arg: args[" + lv_curr_arg + "]=" + args[lv_curr_arg]);
	    if (args[lv_curr_arg].compareTo("-t") == 0) {
		m_table_name = args[++lv_curr_arg];
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_table_name);
	    }
	    else if (args[lv_curr_arg].compareTo("-P") == 0) {
		m_prefix = args[++lv_curr_arg];
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_prefix);
	    }
	    else if (args[lv_curr_arg].compareTo("-n") == 0) {
		m_num_entries = Long.parseLong(args[++lv_curr_arg]);
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_num_entries);
	    }
	    else if (args[lv_curr_arg].compareTo("-b") == 0) {
		m_bucket_id = Integer.parseInt(args[++lv_curr_arg]);
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_bucket_id);
	    }
	    else if (args[lv_curr_arg].compareTo("-K") == 0) {
		m_entered_key = args[++lv_curr_arg];
		m_rk = m_entered_key;
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_rk);
	    }
	    else if (args[lv_curr_arg].compareTo("-k") == 0) {
		m_entered_key = args[++lv_curr_arg];
		m_rk = "rowkey-" + m_entered_key;
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_rk);
	    }
	    else if (args[lv_curr_arg].compareTo("-x") == 0) {
		m_xid = Long.parseLong(args[++lv_curr_arg]);
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_xid);
	    }
	    else if (args[lv_curr_arg].compareTo("-v") == 0) {
		m_verbose = true;
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_verbose);
	    }
	    else if (args[lv_curr_arg].compareTo("-l") == 0) {
		m_load = true;
		System.out.println("Arg[" + lv_curr_arg + "]=" + m_load);
	    }
	    else if (args[lv_curr_arg].compareTo("-h") == 0) {
	        m_locator_host = args[++lv_curr_arg];
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_locator_host);
	    }
	    else if (args[lv_curr_arg].compareTo("-p") == 0) {
	        m_locator_port = Integer.parseInt(args[++lv_curr_arg]);
		if (m_verbose) System.out.println("Arg[" + lv_curr_arg + "]=" + m_locator_host);
	    }
    
	    lv_curr_arg++;
	}

	if (m_validate) {
	    if ((m_rk == null) && (m_bucket_id < 0)) {
		System.out.println("Please provide either a bucket id or a row key");
		return -1;
	    }
	}

	if (m_rk != null) {
	    m_rk_bytes = m_rk.getBytes();
	}

	return 0;
    }

}
