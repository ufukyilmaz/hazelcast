//package com.hazelcast.client.nearcache;
//
//import com.hazelcast.cache.AbstractCacheRecordStore;
//import com.hazelcast.client.spi.ClientContext;
//import com.hazelcast.config.EvictionPolicy;
//import com.hazelcast.config.InMemoryFormat;
//import com.hazelcast.config.NearCacheConfig;
//import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
//import com.hazelcast.monitor.NearCacheStats;
//import com.hazelcast.monitor.impl.NearCacheStatsImpl;
//import com.hazelcast.nio.serialization.Data;
//import com.hazelcast.nio.serialization.DataType;
//import com.hazelcast.nio.serialization.EnterpriseSerializationService;
//import com.hazelcast.spi.Callback;
//
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
///**
// * @author mdogan 18/02/14
// */
//public class ClientOffHeapNearCache<V> implements ClientNearCache<Data, V> {
//
//    private static final int evictionPercentage = 20;
//    private static final int evictionThresholdPercentage = 95;
//
//    private final ConcurrentCacheRecordStore<V> map;
//    private final ScheduledFuture<?> evictionFuture;
//    private final NearCacheStatsImpl stats;
//    private final boolean invalidateOnChange;
//
//    public ClientOffHeapNearCache(String name, ClientContext context) {
//        int cores = Runtime.getRuntime().availableProcessors() * 8;
//        int cl = Math.max(cores, 16);
//        NearCacheConfig cacheConfig = context.getClientConfig().getNearCacheConfig(name);
//        invalidateOnChange = cacheConfig.isInvalidateOnChange();
//        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) context.getSerializationService();
//        if (serializationService.getMemoryManager() == null) {
//            throw new IllegalStateException("OffHeap memory should be enabled and configured " +
//                    "to be able to use offheap NearCache!");
//        }
//        map = new ConcurrentCacheRecordStore<V>(cl, serializationService, cacheConfig);
//
//        evictionFuture = context.getExecutionService()
//                .scheduleWithFixedDelay(new EvictionTask(), 5, 5, TimeUnit.SECONDS);
//        stats = new NearCacheStatsImpl();
//    }
//
//    public void put(Data key, V value) {
//        try {
//            map.set(key, value);
//        } catch (OffHeapOutOfMemoryError ignored) {
//        }
//    }
//
//    public V get(Data key) {
//        try {
//            V value = map.get(key);
//            if (value == null) {
//                stats.incrementMisses();
//            } else {
//                stats.incrementHits();
//            }
//            return value;
//        } catch (OffHeapOutOfMemoryError ignored) {
//            return null;
//        }
//    }
//
//    public void remove(Data key) {
//        try {
//            map.delete(key);
//        } catch (OffHeapOutOfMemoryError ignored) {
//            map.clear();
//        }
//    }
//
//    public void invalidate(Data key) {
//        remove(key);
//    }
//
//    public void clear() {
//        map.clear();
//    }
//
//    public void destroy() {
//        evictionFuture.cancel(true);
//        map.destroy();
//    }
//
//    @Override
//    public boolean isInvalidateOnChange() {
//        return invalidateOnChange;
//    }
//
//    @Override
//    public InMemoryFormat getInMemoryFormat() {
//        return InMemoryFormat.OFFHEAP;
//    }
//
//    @Override
//    public void setId(String id) {
//    }
//
//    @Override
//    public String getId() {
//        return null;
//    }
//
//    @Override
//    public NearCacheStats getNearCacheStats() {
//        stats.setOwnedEntryCount(calculateSize());
//        stats.setOwnedEntryMemoryCost(0);
//        return stats;
//    }
//
//    private long calculateSize() {
//        long res = 0;
//        for (ConcurrentCacheRecordStore.Segment segment : map.segments) {
//            if (Thread.currentThread().isInterrupted()) {
//                return 0;
//            }
//            segment.lock();
//            try {
//                res += segment.getSize();
//            } finally {
//                segment.unlock();
//            }
//        }
//        return res;
//    }
//
//    private class EvictionTask implements Runnable {
//        public void run() {
//            for (ConcurrentCacheRecordStore.Segment segment : map.segments) {
//                if (Thread.currentThread().isInterrupted()) {
//                    return;
//                }
//                if (segment.hasTTL()) {
//                    segment.lock();
//                    try {
//                        segment.evictExpired();
//                    } finally {
//                        segment.unlock();
//                    }
//                }
//            }
//        }
//    }
//
//    private static class ConcurrentCacheRecordStore<V> {
//
//        private final EnterpriseSerializationService ss;
//
//        private final int segmentMask;
//
//        private final int segmentShift;
//
//        private final Segment[] segments;
//
//        private Segment segmentFor(Data key) {
//            return segments[(key.hashCode() >>> segmentShift) & segmentMask];
//        }
//
//        public ConcurrentCacheRecordStore(int concurrencyLevel,
//                EnterpriseSerializationService ss, NearCacheConfig cacheConfig) {
//            // Find power-of-two sizes best matching arguments
//            int sshift = 0;
//            int ssize = 1;
//            while (ssize < concurrencyLevel) {
//                ++sshift;
//                ssize <<= 1;
//            }
//            segmentShift = 32 - sshift;
//            segmentMask = ssize - 1;
//
//            segments = new Segment[ssize];
//            for (int i = 0; i < ssize; i++) {
//                segments[i] = new Segment(ss, cacheConfig);
//            }
//            this.ss = ss;
//        }
//
//        public void set(Data key, V value) {
//            Data v = ss.toData(value, DataType.OFFHEAP);
//            Segment segment = segmentFor(key);
//            segment.set(key, v, -1);
//        }
//
//        public void delete(Data key) {
//            Segment segment = segmentFor(key);
//            segment.delete(key);
//        }
//
//        public V get(Data key) {
//            Segment segment = segmentFor(key);
//            try {
//                Data value = segment.getValue(key);
//                return ss.toObject(value);
//            } catch (InterruptedException ignored) {
//            }
//            return null;
//        }
//
//        public void clear() {
//            for (Segment segment : segments) {
//                segment.lock();
//                try {
//                    segment.clear();
//                } finally {
//                    segment.unlock();
//                }
//            }
//        }
//
//        public void destroy() {
//            for (Segment segment : segments) {
//                segment.lock.lock();
//                try {
//                    segment.destroy();
//                } finally {
//                    segment.lock.unlock();
//                }
//            }
//        }
//
//        protected static final class Segment extends AbstractCacheRecordStore {
//
//            final Lock lock = new ReentrantLock();
//
//            Segment(EnterpriseSerializationService ss, NearCacheConfig cacheConfig) {
//                super(ss, 500, cacheConfig.getTimeToLiveSeconds(),
//                        EvictionPolicy.valueOf(cacheConfig.getEvictionPolicy()),
//                        evictionPercentage, evictionThresholdPercentage);
//            }
//
//            private Data getValue(Data key) throws InterruptedException {
//                if (lock.tryLock(25, TimeUnit.MILLISECONDS)) {
//                    try {
//                        return get(key);
//                    } finally {
//                        lock.unlock();
//                    }
//                }
//                return null;
//            }
//
//            private boolean containsKey(Data key) throws InterruptedException {
//                if (lock.tryLock(25, TimeUnit.MILLISECONDS)) {
//                    try {
//                        return contains(key);
//                    } finally {
//                        lock.unlock();
//                    }
//                }
//                return false;
//            }
//
//            protected void set(Data key, Data value, long ttlMillis) {
//                lock.lock();
//                try {
//                    put(key, value, ttlMillis, null);
//                } finally {
//                    lock.unlock();
//                }
//            }
//
//            protected void delete(Data key) {
//                lock.lock();
//                try {
//                    remove(key, null);
//                } finally {
//                    lock.unlock();
//                }
//            }
//
//            int getSize() {
//                return map.size();
//            }
//
//            protected void onClear() {
//            }
//
//            protected void onDestroy() {
//            }
//
//            protected void onEntryInvalidated(Data key, String source) {
//            }
//
//            protected Callback<Data> createEvictionCallback() {
//                return null;
//            }
//
//            void lock() {
//                lock.lock();
//            }
//
//            void unlock() {
//                lock.unlock();
//            }
//
//            void evictExpired() {
//                map.evictExpiredRecords(10);
//            }
//        }
//    }
//
//}
