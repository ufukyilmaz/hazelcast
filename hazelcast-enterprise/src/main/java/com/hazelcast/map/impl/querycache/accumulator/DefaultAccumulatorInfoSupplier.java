package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Default implementation of {@link AccumulatorInfoSupplier}
 * <p/>
 * At most one thread can write to this class at a time.
 *
 * @see AccumulatorInfoSupplier
 */
public class DefaultAccumulatorInfoSupplier implements AccumulatorInfoSupplier {

    private static final ConstructorFunction<String, ConcurrentMap<String, AccumulatorInfo>> INFO_CTOR
            = new ConstructorFunction<String, ConcurrentMap<String, AccumulatorInfo>>() {
        @Override
        public ConcurrentMap<String, AccumulatorInfo> createNew(String arg) {
            return new ConcurrentHashMap<String, AccumulatorInfo>();
        }
    };

    private final ConcurrentMap<String, ConcurrentMap<String, AccumulatorInfo>> cacheInfoPerMap;

    public DefaultAccumulatorInfoSupplier() {
        this.cacheInfoPerMap = new ConcurrentHashMap<String, ConcurrentMap<String, AccumulatorInfo>>();
    }

    @Override
    public AccumulatorInfo getAccumulatorInfoOrNull(String mapName, String cacheName) {
        ConcurrentMap<String, AccumulatorInfo> cacheToInfoMap = cacheInfoPerMap.get(mapName);
        return cacheToInfoMap.get(cacheName);
    }

    @Override
    public void putIfAbsent(String mapName, String cacheName, AccumulatorInfo info) {
        ConcurrentMap<String, AccumulatorInfo> cacheToInfoMap = getOrPutIfAbsent(cacheInfoPerMap, mapName, INFO_CTOR);
        cacheToInfoMap.putIfAbsent(cacheName, info);
    }

    @Override
    public void remove(String mapName, String cacheName) {
        ConcurrentMap<String, AccumulatorInfo> cacheToInfoMap = cacheInfoPerMap.get(mapName);
        if (cacheToInfoMap == null) {
            return;
        }
        cacheToInfoMap.remove(cacheName);
    }
}
