package io.esgyn.utils;

import io.ampool.monarch.table.*;
import io.ampool.monarch.table.exceptions.*;

import io.esgyn.coprocessor.EsgynMGet;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GeneralUtils {

    static final Logger LOG = LogManager.getLogger(GeneralUtils.class.getName());

    static public boolean listContains(List<byte[]> pv_list,
				       byte[]       pv_element,
				       boolean      pv_remove_element_from_list_if_found) 
    {

	ListIterator<byte[]> lv_li = null;
	for (lv_li = pv_list.listIterator();
	     lv_li.hasNext();
	     ) {
	
	    byte[] lv_curr = lv_li.next();
	    if (Arrays.equals(lv_curr, pv_element)) {
		if (pv_remove_element_from_list_if_found) {
		    lv_li.remove();
		}
		return  true;
	    }
	}

	return false;
    }

  /* increment/deincrement for positive value */
  /* This method copied from o.a.h.h.utils.Bytes */
  public static byte [] binaryIncrementPos(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = value[value.length-i-1] & 0x0ff;
      int total = val + cur;
      if(total > 255) {
        amo += sign;
        total %= 256;
      } else if (total < 0) {
        amo -= sign;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

}