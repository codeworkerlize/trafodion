package org.apache.hadoop.hbase.coprocessor.transactional.message;

import lombok.Getter;

import java.util.Properties;
import java.util.Map.Entry;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.OrderedProperties;

public class RSParameterMessage extends RSMessage {
    private static final long serialVersionUID = 1L;
    @Getter
    private Map<String, OrderedProperties> parameters;

    public RSParameterMessage() {
        super(MSG_TYP_GET_RS_PARAMETER_RESPONSE);
        parameters = new HashMap<String, OrderedProperties>();
    }

    @Override
    public void merge(RSMessage msg) {
        Map<String, OrderedProperties> inMap = ((RSParameterMessage)msg).getParameters();
        if (!inMap.isEmpty()) {
            this.parameters.putAll(inMap);
        }
    }

    public void addNewProperties(String serverName, OrderedProperties props) {
        this.parameters.put(serverName, props);
    }

    public void displayAllParameters(boolean getOnlyLockStatus) {
        StringBuffer result = new StringBuffer();
        if (getOnlyLockStatus) {
            result.append("row lock status on each server:\n");
            StringUtil.generateHeader(result, LockConstants.LOCK_ENABLE_ROW_LOCK_HEADER);
            List<StringBuffer> rows = new ArrayList<>();
            for (String regionName : parameters.keySet()) {
                StringUtil.formatOutputWide(rows, 1, regionName);
                StringUtil.formatOutputWide(rows, 2, parameters.get(regionName).getProperty("ENABLE_ROW_LEVEL_LOCK"));
                for (StringBuffer row : rows) {
                    result.append(row).append("\n");
                }
                rows.clear();
            }
            System.out.println(result.toString());
            result.delete(0, result.length());
        } else {
            System.out.println("configuration for regionServer on each server");
            for (Map.Entry<String, OrderedProperties> entry : parameters.entrySet()) {
                System.out.println(entry.getKey());
                System.out.println("----------------------------------------------------------------------");
                //System.out.println(entry.getValue()); System.out.println(Properties) lacks line break
                //for (Entry<Object, Object> item : entry.getValue().entrySet()) {
                OrderedProperties prop = entry.getValue();
                Iterator<String> itor = prop.stringPropertyNames().iterator();
                while (itor.hasNext()) {
                    String key = itor.next();
                    System.out.println(key + "=" + prop.get(key));
                }
                System.out.println("----------------------------------------------------------------------");
            }
        }
    }

    public void getAllParametersJson(boolean getOnlyLockStatus) {
        JSONObject json = new JSONObject();
        if (getOnlyLockStatus) {
            //enable row lock details
            JSONArray jsLockStatus = new JSONArray();
            for (String regionName : parameters.keySet()) {
                JSONObject item = new JSONObject();
                item.put(regionName, parameters.get(regionName).getProperty("ENABLE_ROW_LEVEL_LOCK"));
                jsLockStatus.put(item);
            }
            json.put("lockStatus", jsLockStatus);
        } else {
            //lock configuration on each server
            JSONArray jsServerList = new JSONArray();
            for (Map.Entry<String, OrderedProperties> entry : parameters.entrySet()) {
                JSONObject server = new JSONObject();
                //region name
                server.put("serverName", entry.getKey());
                //configuration
                JSONArray jsConfig = new JSONArray();
                {
                    for (Entry<Object, Object> item : entry.getValue().entrySet()) {
                        JSONObject kv = new JSONObject();
                        kv.put("key", item.getKey().toString());
                        kv.put("value", (OrderedProperties) item.getValue());
                        jsConfig.put(kv);
                    }
                }
                server.put("lockConfigs", jsConfig);
                jsServerList.put(server);
            }
            json.put("serverConfigInfo", jsServerList);
        }
        System.out.println(json.toString());
    }
}
