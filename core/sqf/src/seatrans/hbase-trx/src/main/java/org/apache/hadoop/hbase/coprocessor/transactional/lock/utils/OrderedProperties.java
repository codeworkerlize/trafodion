package org.apache.hadoop.hbase.coprocessor.transactional.lock.utils;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;
import java.util.LinkedHashSet;

public class OrderedProperties extends Properties {
    private static final long serialVersionUID = 123L;
    //no HashSet(HashMap)
    private final LinkedHashSet<Object> keys = new LinkedHashSet<>();

    @Override
    public Enumeration<Object> keys() {
        return Collections.<Object>enumeration(keys);
    }

    @Override
    public Object put(Object key, Object value) {
        keys.add(key);
        return super.put(key, value);
    }

    @Override
    public Set<Object> keySet() {
        return keys;
    }

    @Override
    public Set<String> stringPropertyNames() {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (Object key : keys)
            set.add((String) key);
        return set;
    }
}