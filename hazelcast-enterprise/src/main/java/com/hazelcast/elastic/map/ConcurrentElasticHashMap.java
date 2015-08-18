package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
* @author mdogan 07/01/14
*/
public class ConcurrentElasticHashMap<K, V> implements ConcurrentElasticMap<K, V> {

    /**
     * The default concurrency level for this table, used when not
     * otherwise specified in a constructor.
     */
    static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum number of segments to allow; used to bound
     * constructor arguments.
     */
    private static final int MAX_SEGMENTS = 1 << 16;

    private final EnterpriseSerializationService ss;

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    private final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    private final Segment<V>[] segments;

    /**
     * Returns the segment that should be used for key with given hash
     * @param key the key
     * @return the segment
     */
    protected final Segment<V> segmentFor(Data key) {
        return segments[(key.hashCode() >>> segmentShift) & segmentMask];
    }

    public ConcurrentElasticHashMap(EnterpriseSerializationService ss, MemoryAllocator malloc) {
        this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, ss, malloc);
    }

    public ConcurrentElasticHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
            EnterpriseSerializationService ss, MemoryAllocator malloc) {

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;

        int c = initialCapacity / ssize;
        if (c * ssize < initialCapacity)
            ++c;
        int cap = 1;
        while (cap < c) {
            cap <<= 1;
        }
        segments = new Segment[ssize];
        for (int i = 0; i < ssize; i++) {
            segments[i] = new Segment(ss, cap, loadFactor, malloc);
        }
        this.ss = ss;
    }

    protected static final class Segment<V> {

        final ReentrantLock lock = new ReentrantLock();
        final EnterpriseSerializationService ss;
        final BinaryElasticHashMap<NativeMemoryData> map;

//        /**
//         * The number of elements in this segment's region.
//         */
//        transient volatile int count;

        Segment(EnterpriseSerializationService ss, int initialCapacity, float loadFactor, MemoryAllocator malloc) {
            this.ss = ss;
            map = new BinaryElasticHashMap<NativeMemoryData>(initialCapacity, loadFactor,
                    ss, new NativeMemoryDataAccessor(ss), malloc);
        }

        public V put(Data key, V value) {
            NativeMemoryData v = ss.toData(value, DataType.NATIVE);
            try {
                NativeMemoryData old = null;
                lock.lock();
                try {
                    old = map.put(key, v);
                } finally {
                    lock.unlock();
                }
                if (old != null) {
                    try {
                        return (V) ss.toObject(old);
                    } finally {
                        ss.disposeData(old);
                    }
                }
            } catch (NativeOutOfMemoryError e) {
                ss.disposeData(v);
                throw e;
            }
            return null;
        }

        public boolean set(Data key, V value) {
            NativeMemoryData v = ss.toData(value, DataType.NATIVE);
            try {
                lock.lock();
                try {
                    return map.set(key, v);
                } finally {
                    lock.unlock();
                }
            } catch (NativeOutOfMemoryError e) {
                ss.disposeData(v);
                throw e;
            }
        }

        public V putIfAbsent(final Data key, V value) {
            NativeMemoryData v = ss.toData(value, DataType.NATIVE);
            try {
                lock.lock();
                try {
                    NativeMemoryData current = map.putIfAbsent(key, v);
                    if (current != null) {
                        ss.disposeData(key);
                        ss.disposeData(v);
                        return (V) ss.toObject(current);
                    }
                    return null;
                } finally {
                    lock.unlock();
                }
            } catch (NativeOutOfMemoryError e) {
                ss.disposeData(v);
                throw e;
            }
        }

        public boolean replace(final Data key, final V oldValue, final V newValue) {
            NativeMemoryData o = null;
            NativeMemoryData n = null;

            try {
                o = ss.toData(oldValue, DataType.NATIVE);
                n = ss.toData(newValue, DataType.NATIVE);

                boolean replaced = false;
                lock.lock();
                try {
                    replaced = map.replace(key, o, n);
                } finally {
                    lock.unlock();
                }

                ss.disposeData(o);
                if (!replaced) {
                    ss.disposeData(n);
                }
                return replaced;
            } catch (NativeOutOfMemoryError e) {
                ss.disposeData(o);
                ss.disposeData(n);
                throw e;
            }
        }

        public V replace(final Data key, V value) {
            NativeMemoryData v = ss.toData(value, DataType.NATIVE);

            try {
                NativeMemoryData old = null;
                lock.lock();
                try {
                    old = map.replace(key, v);
                } finally {
                    lock.unlock();
                }

                if (old != null) {
                    try {
                        return (V) ss.toObject(old);
                    } finally {
                        ss.disposeData(old);
                    }
                } else {
                    ss.disposeData(v);
                    return null;
                }
            } catch (NativeOutOfMemoryError e) {
                ss.disposeData(v);
                throw e;
            }
        }

        public V remove(Data key) {
            NativeMemoryData old = null;

            lock.lock();
            try {
                old = map.remove(key);
            } finally {
                lock.unlock();
            }

            if (old != null) {
                try {
                    return (V) ss.toObject(old);
                } finally {
                    ss.disposeData(old);
                }
            }
            return null;
        }

        public boolean delete(Data key) {
            lock.lock();
            try {
                return map.delete(key);
            } finally {
                lock.unlock();
            }
        }

        public boolean remove(final Data key, final V value) {
            Data v = ss.toData(value, DataType.NATIVE);

            try {
                lock.lock();
                try {
                    try {
                        return map.remove(key, v);
                    } finally {
                        ss.disposeData(v);
                    }
                } finally {
                    lock.unlock();
                }
            } catch (NativeOutOfMemoryError e) {
                ss.disposeData(v);
                throw e;
            }
        }

        public V get(Data key) {
            lock.lock();
            try {
                NativeMemoryData v = map.get(key);
                return v != null ? (V) ss.toObject(v) : null;
            } finally {
                lock.unlock();
            }
        }

        public boolean containsKey(Data key) {
            lock.lock();
            try {
                return map.containsKey(key);
            } finally {
                lock.unlock();
            }
        }

        public boolean containsValue(final Data value) {
            lock.lock();
            try {
                return map.containsValue(value);
            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            lock.lock();
            try {
                map.clear();
            } finally {
                lock.unlock();
            }
        }

        public void destroy() {
            lock.lock();
            try {
                map.destroy();
            } finally {
                lock.unlock();
            }
        }

        public int size() {
            lock.lock();
            try {
                return map.size();
            } finally {
                lock.unlock();
            }
        }

        public boolean isEmpty() {
            lock.lock();
            try {
                return map.isEmpty();
            } finally {
                lock.unlock();
            }
        }
    }


    @Override
    public V put(K key, V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        try {
            Segment<V> segment = segmentFor(k);
            return segment.put(k, value);
        } catch (NativeOutOfMemoryError e) {
            ss.disposeData(k);
            throw e;
        }
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean set(K key, V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        try {
            Segment<V> segment = segmentFor(k);
            return segment.set(k, value);
        } catch (NativeOutOfMemoryError e) {
            ss.disposeData(k);
            throw e;
        }
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        try {
            Segment<V> segment = segmentFor(k);
            return segment.putIfAbsent(k, value);
        } catch (NativeOutOfMemoryError e) {
            ss.disposeData(k);
            throw e;
        }
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        Segment<V> segment = segmentFor(k);
        try {
            return segment.replace(k, oldValue, newValue);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public V replace(final K key, final V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        Segment<V> segment = segmentFor(k);
        try {
            return segment.replace(k, value);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public V get(Object key) {
        Data k = ss.toData(key, DataType.NATIVE);
        Segment<V> segment = segmentFor(k);
        try {
            return segment.get(k);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public V remove(Object key) {
        Data k = ss.toData(key, DataType.NATIVE);
        Segment<V> segment = segmentFor(k);
        try {
            return segment.remove(k);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public boolean delete(K key) {
        Data k = ss.toData(key, DataType.NATIVE);
        Segment segment = segmentFor(k);
        try {
            return segment.delete(k);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        Data k = ss.toData(key, DataType.NATIVE);
        Segment<V> segment = segmentFor(k);
        try {
            return segment.remove(k, (V) value);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        Data k = ss.toData(key, DataType.NATIVE);
        Segment segment = segmentFor(k);
        try {
            return segment.containsKey(k);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public boolean containsValue(final Object value) {
        Data v = ss.toData(value, DataType.NATIVE);
        try {
            for (Segment segment : segments) {
                if (segment.containsValue(v)) {
                    return true;
                }
            }
            return false;
        } finally {
            ss.disposeData(v);
        }
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        int s = 0;
        for (Segment segment : segments) {
            s += segment.size();
        }
        return s;
    }

    @Override
    public boolean isEmpty() {
        for (Segment segment : segments) {
            if (!segment.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        for (Segment segment : segments) {
            segment.clear();
        }
    }

    @Override
    public void destroy() {
        for (int i = 0; i < segments.length; i++) {
            segments[i].destroy();
            segments[i] = null;
        }
    }
}
