package org.trafodion.jdbc.t4.trace;

import java.util.HashMap;

public class TraceAverageUsedTime {

    private HashMap<String,InnerTime> map;

    public TraceAverageUsedTime(){
        map = new HashMap<String,InnerTime>();
    }

    public void setTime(String key, long usedTime){
        InnerTime inner = map.get(key);
        if (inner == null) {
            inner = new InnerTime(usedTime);
            map.put(key,inner);
        }else{
            inner.add(usedTime);
        }

    }

    public long get(String key){
        InnerTime inner = map.get(key);
        return inner.getAverage();
    }

    public int getCount(String key){
        InnerTime inner = map.get(key);
        return inner.getCount();
    }

    public void clear(){
        map.clear();
    }

    private class InnerTime{
        private long totalTime;
        private int count;

        private InnerTime(long time){
            totalTime = time;
            count = 1;
        }

        private void add(long time){
            totalTime += time;
            count ++;
        }

        private long getAverage(){
            if (count > 0) {
                return totalTime/count;
            }
            return 0;
        }

        private int getCount(){
            return count;
        }
    }


}
