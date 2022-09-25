package org.apache.hadoop.hbase.coprocessor.transactional.lock.cache;

import java.util.Vector;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockSizeof;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;

public class VectorCache<E> implements LockSizeof {
    private Vector<E> cache;
    private int cacheSize;

    public VectorCache(int cacheSize) {
      this.cacheSize = cacheSize;
      if (cacheSize > 0) {
          this.cache = new Vector<>(cacheSize);
      }
    }

    public synchronized boolean add(E e) {
        if (cacheSize == 0 || cache.size() >= cacheSize) {
            return false;
        }
        return cache.add(e);
    }

    public synchronized E getElement() {
        if (cacheSize == 0 || cache.size() == 0) {
            return null;
        }
        E e = cache.lastElement();
        cache.removeElementAt(cache.size() - 1);
        return e;
    }

    public int size() {
        return cacheSize;
    }

    public synchronized int cachedSize() {
        return cache.size();
    }

    public synchronized void changeCacheSize(int size) {
        if (size == 0 || size == this.cacheSize) {
            return;
        }
        if (this.cache == null) {
            this.cache = new Vector<>(size);
        } else if (size <= this.cache.size()) {
            this.cache.setSize(size);
            this.cache.trimToSize();
        }
        /*
        else if (size > this.cacheSize) {
            this.cache.ensureCapacity(size);
            //will remove all existing instances!
        }
        */
        this.cacheSize = size;
    }

    public synchronized void clear() {
        this.cache.clear();
        this.cache.trimToSize();
    }

    @Override
    public long sizeof() {
        long size = Size_Object;
        //2 attributes
        size += 2 * Size_Reference;
        //1 int
        size += 4;
        //1 Vector
        size += Size_Vector;
        //cache
        synchronized (this) {
            if (!this.cache.isEmpty()) {
                E e = cache.firstElement();
                long entrySize = 16;//Size_Object + (Size_Object % 8)
                if (e instanceof LockSizeof) {
                    entrySize = ((LockSizeof) e).sizeof();
                }
                size += (entrySize + Size_Reference) * cache.size();
            }
        }
        return LockUtils.alignLockObjectSize(size);
    }
}
