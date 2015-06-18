package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * @author mdogan 07/01/14
 */
public final class ElasticHashMap<K, V> implements ElasticMap<K, V> {

    private final BinaryElasticHashMap<NativeMemoryData> map;
    private final EnterpriseSerializationService ss;

    public ElasticHashMap(EnterpriseSerializationService ss, MemoryAllocator malloc) {
        this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, ss, malloc);
    }

    public ElasticHashMap(int initialCapacity, float loadFactor, EnterpriseSerializationService ss,
            MemoryAllocator malloc) {
        this.ss = ss;
        map = new BinaryElasticHashMap<NativeMemoryData>(initialCapacity, loadFactor,
                ss, new NativeMemoryDataAccessor(ss), malloc);
    }

    @Override
    public V put(K key, V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData v = ss.toData(value, DataType.NATIVE);
        NativeMemoryData old;
        old = map.put(k, v);
        if (old != null) {
            try {
                return (V) ss.toObject(old);
            } finally {
                ss.disposeData(old);
            }
        }
        return null;
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
        NativeMemoryData v = ss.toData(value, DataType.NATIVE);
        return map.set(k, v);
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData v = ss.toData(value, DataType.NATIVE);
        NativeMemoryData current = map.putIfAbsent(k, v);
        if (current != null) {
            ss.disposeData(k);
            ss.disposeData(v);
            return (V) ss.toObject(current);
        }
        return null;
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData o = ss.toData(oldValue, DataType.NATIVE);
        NativeMemoryData n = ss.toData(newValue, DataType.NATIVE);
        boolean replaced = map.replace(k, o, n);
        ss.disposeData(k);
        ss.disposeData(o);
        if (!replaced) {
            ss.disposeData(n);
        }
        return replaced;
    }

    @Override
    public V replace(final K key, final V value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData v = ss.toData(value, DataType.NATIVE);
        NativeMemoryData old = map.replace(k, v);
        ss.disposeData(k);
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
    }

    @Override
    public V get(Object key) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData v = map.get(k);
        ss.disposeData(k);
        return v != null ? (V) ss.toObject(v) : null;
    }

    @Override
    public V remove(Object key) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData old = map.remove(k);
        ss.disposeData(k);

        if (old != null) {
            try {
                return (V) ss.toObject(old);
            } finally {
                ss.disposeData(old);
            }
        }
        return null;
    }

    @Override
    public boolean delete(K key) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        try {
            return map.delete(k);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        NativeMemoryData v = ss.toData(value, DataType.NATIVE);
        try {
            return map.remove(k, v);
        } finally {
            ss.disposeData(k);
            ss.disposeData(v);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        NativeMemoryData k = ss.toData(key, DataType.NATIVE);
        try {
            return map.containsKey(k);
        } finally {
            ss.disposeData(k);
        }
    }

    @Override
    public boolean containsValue(final Object value) {
        NativeMemoryData v = ss.toData(value, DataType.NATIVE);
        try {
            return map.containsValue(v);
        } finally {
            ss.disposeData(v);
        }
    }

    public void clear() {
        map.clear();
    }

    public void destroy() {
        map.destroy();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
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
}
