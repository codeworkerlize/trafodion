package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;

import io.esgyn.coprocessor.*;
import io.esgyn.utils.AmpoolUtils;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.client.transactional.TransactionState;


// Singleton TransactionState map.
//
public class MTransactionMap {
  private static final ConcurrentHashMap<Long, MTransactionState> g_mapTransactionStates
                       = new ConcurrentHashMap<Long, MTransactionState>();
  
  public MTransactionMap() {
    //System.out.println("TransactionMap ctor");
  }
  public static synchronized ConcurrentHashMap<Long, MTransactionState> getInstance() {
    return g_mapTransactionStates;
  }
}
