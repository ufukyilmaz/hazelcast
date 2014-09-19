package com.hazelcast.cache;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 05/02/14
 */
final class CacheProxy extends AbstractDistributedObject<CacheService> implements ICache {

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private CacheStatsImpl stats;
    final String name;

    protected CacheProxy(String name, NodeEngine nodeEngine, CacheService service) {
        super(nodeEngine, service);
        this.name = name;
        this.stats = service.getOrCreateCacheStats(name);
    }

    @Override
    public Object get(Object key) {
        long start = System.currentTimeMillis();
        Future<Object> f = getAsyncInternal(key);
        try {
            final Object value = f.get();
            if (value != null) {
                stats.updateGetStats(true, start);
            } else {
                stats.updateGetStats(false, start);
            }
            return value;
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public Future<Object> getAsync(Object key) {
        stats.updateAsyncGetStats();
        return getAsyncInternal(key);
    }

    private Future<Object> getAsyncInternal(Object key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Operation op = new CacheGetOperation(name, k);
        InternalCompletableFuture<Object> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));

        return new DelegatingFuture<Object>(f, serializationService);
    }

    @Override
    public Map getAll(Set keys) {
        Map map = new HashMap(keys.size());
        for (Object key : keys) {
            map.put(key, get(key));
        }
        return map;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Operation op = new CacheContainsKeyOperation(name, k);
        InternalCompletableFuture<Boolean> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
        stats.setLastAccessTime(System.currentTimeMillis());
        return f.getSafely();
    }

    @Override
    public void put(Object key, Object value) {
        long start = System.currentTimeMillis();
        putAsyncInternal(key, value, -1, false).getSafely();
        stats.updatePutStats(start);
    }

    @Override
    public void put(Object key, Object value, long ttl, TimeUnit timeUnit) {
        if (ttl <= 0) {
            throw new IllegalArgumentException("TTL must be positive!");
        }
        long start = System.currentTimeMillis();
        putAsyncInternal(key, value, getTtlMillis(ttl, timeUnit), false).getSafely();
        stats.updatePutStats(start);
    }

    @Override
    public Future<Void> putAsync(Object key, Object value) {
        final InternalCompletableFuture<Void> future = putAsyncInternal(key, value, -1, false);
        stats.updateAsyncPutStats();
        return future;
    }

    @Override
    public Future<Void> putAsync(Object key, Object value, long ttl, TimeUnit timeUnit) {
        if (ttl <= 0) {
            throw new IllegalArgumentException("TTL must be positive!");
        }
        final InternalCompletableFuture<Void> future = putAsyncInternal(key, value, getTtlMillis(ttl, timeUnit), false);
        stats.updateAsyncPutStats();
        return future;
    }

    @Override
    public Object getAndPut(Object key, Object value) {
        long start = System.currentTimeMillis();
        Future<Object> f = getAndPutAsyncInternal(key, value);
        try {
            final Object oldValue = f.get();
            stats.updatePutStats(start);
            return oldValue;
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public Object getAndPut(Object key, Object value, long ttl, TimeUnit timeUnit) {
        if (ttl <= 0) {
            throw new IllegalArgumentException("TTL must be positive!");
        }
        long start = System.currentTimeMillis();
        Future<Object> f = getAndPutAsyncInternal(key, value, ttl, timeUnit);
        try {
            final Object oldValue = f.get();
            stats.updatePutStats(start);
            return oldValue;
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public Future<Object> getAndPutAsync(Object key, Object value) {
        stats.updateAsyncPutStats();
        return getAndPutAsyncInternal(key, value);
    }

    private Future<Object> getAndPutAsyncInternal(Object key, Object value) {
        NodeEngine nodeEngine = getNodeEngine();
        InternalCompletableFuture<Object> f = putAsyncInternal(key, value, -1, true);
        SerializationService serializationService = getService().getSerializationService();
        return new DelegatingFuture<Object>(f, serializationService);
    }

    @Override
    public Future<Object> getAndPutAsync(Object key, Object value, long ttl, TimeUnit timeUnit) {
        stats.updateAsyncPutStats();
        return getAndPutAsyncInternal(key, value, ttl, timeUnit);
    }

    private Future<Object> getAndPutAsyncInternal(Object key, Object value, long ttl, TimeUnit timeUnit) {
        if (ttl <= 0) {
            throw new IllegalArgumentException("TTL must be positive!");
        }
        NodeEngine nodeEngine = getNodeEngine();
        InternalCompletableFuture<Object> f = putAsyncInternal(key, value, getTtlMillis(ttl, timeUnit), true);
        SerializationService serializationService = getService().getSerializationService();
        return new DelegatingFuture<Object>(f, serializationService);
    }

    private <T> InternalCompletableFuture<T> putAsyncInternal(Object key, Object value,
            long ttlMillis, boolean getValue) {

        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Data v = serializationService.toData(value, DataType.HEAP);

        Operation op = new CachePutOperation(name, k, v, ttlMillis, getValue);
        return engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
    }

    @Override
    public boolean putIfAbsent(Object key, Object value) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        long start = System.currentTimeMillis();
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Data v = serializationService.toData(value, DataType.HEAP);

        Operation op = new CachePutIfAbsentOperation(name, k, v);
        InternalCompletableFuture<Boolean> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));

        final Boolean result = f.getSafely();
        stats.updatePutStats(start);
        return result;
    }

    @Override
    public void putAll(Map map) {
        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            long start = System.currentTimeMillis();
            Map.Entry next = (Map.Entry) iter.next();
            putAsyncInternal(next.getKey(), next.getValue(), -1, false);
            stats.updatePutStats(start);
        }
    }

    @Override
    public boolean remove(Object key) {
        long start = System.currentTimeMillis();
        InternalCompletableFuture<Boolean> f = removeAsyncInternal(key);
        final Boolean result = f.getSafely();
        stats.updateRemoveStats(start);
        return result;
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(Object key) {
        stats.updateAsyncRemoveStats();
        return removeAsyncInternal(key);
    }

    private InternalCompletableFuture<Boolean> removeAsyncInternal(Object key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);

        Operation op = new CacheRemoveOperation(name, k);
        return engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
    }

    @Override
    public Object getAndRemove(Object key) {
        long start = System.currentTimeMillis();
        Future<Object> f = getAndRemoveAsyncInternal(key);
        try {
            final Object value = f.get();
            stats.updateRemoveStats(start);
            return value;
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public Future<Object> getAndRemoveAsync(Object key) {
        stats.updateAsyncRemoveStats();
        return getAndRemoveAsyncInternal(key);
    }

    private Future<Object> getAndRemoveAsyncInternal(Object key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);

        Operation op = new CacheGetAndRemoveOperation(name, k);
        InternalCompletableFuture<Object> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
        return new DelegatingFuture<Object>(f, serializationService);
    }

    @Override
    public boolean remove(Object key, Object oldValue) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        long start = System.currentTimeMillis();
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Data old = serializationService.toData(oldValue, DataType.HEAP);

        Operation op = new CacheRemoveOperation(name, k, old);
        InternalCompletableFuture<Boolean> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));

        final Boolean result = f.getSafely();
        stats.updateRemoveStats(start);
        return result;
    }

    @Override
    public void removeAll(Set keys) {
        for (Object key : keys) {
            long start = System.currentTimeMillis();
            removeAsyncInternal(key);
            stats.updateRemoveStats(start);
        }
    }

    @Override
    public void removeAll() {
        clear();
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        if (oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        return replaceInternal(key, oldValue, newValue);
    }

    @Override
    public boolean replace(Object key, Object value) {
        return replaceInternal(key, null, value);
    }

    private boolean replaceInternal(Object key, Object oldValue, Object newValue) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (newValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Data o = serializationService.toData(oldValue, DataType.HEAP);
        Data n = serializationService.toData(newValue, DataType.HEAP);

        Operation op = new CacheReplaceOperation(name, k, o, n);
        InternalCompletableFuture<Boolean> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
        return f.getSafely();
    }

    @Override
    public Object getAndReplace(Object key, Object value) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        long start = System.currentTimeMillis();
        NodeEngine engine = getNodeEngine();
        EnterpriseSerializationService serializationService = getService().getSerializationService();

        Data k = serializationService.toData(key, DataType.HEAP);
        Data v = serializationService.toData(value, DataType.HEAP);

        Operation op = new CacheGetAndReplaceOperation(name, k, v);
        InternalCompletableFuture<Object> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));

        Object result = f.getSafely();
        final Object oldValue = serializationService.toObject(result);
        stats.updatePutStats(start);
        return oldValue;
    }

    private static int getPartitionId(NodeEngine nodeEngine, Data key) {
        return nodeEngine.getPartitionService().getPartitionId(key);
    }

    private static long getTtlMillis(long ttl, TimeUnit timeUnit) {
        return timeUnit != null ? timeUnit.toMillis(ttl) : ttl;
    }

    @Override
    public int size() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            SerializationService serializationService = getService().getSerializationService();
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(getServiceName(), new CacheSizeOperationFactory(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size;
                if (result instanceof Data) {
                    size = serializationService.toObject((Data) result);
                } else {
                    size = (Integer) result;
                }
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public CacheStats getStats() {
        stats.setOwnedEntryCount(calculateOwnedEntryCount());
        return stats;
    }

    private long calculateOwnedEntryCount() {
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        final Address thisAddress = getNodeEngine().getClusterService().getThisAddress();
        final InternalPartition[] partitions = partitionService.getPartitions();
        long ownedEntryCount = 0;
        for (InternalPartition partition : partitions) {
            if (thisAddress.equals(partition.getOwnerOrNull())) {
                final CacheRecordStore cache = getService().getCache(name, partition.getPartitionId());
                if (cache != null) {
                    ownedEntryCount += cache.size();
                }
            }
        }
        return ownedEntryCount;
    }

    @Override
    public void clear() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(getServiceName(), new CacheClearOperationFactory(name));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public Iterator<Entry> iterator() {
        return iterator(100);
    }

    public Iterator<Entry> iterator(int fetchCount) {
        return new CacheIterator(this, getNodeEngine(), fetchCount);
    }

    @Override
    public void close() {
        destroy();
    }

    @Override
    public Object unwrap(Class clazz) {
        if (clazz.equals(ICache.class)) {
            return this;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Configuration getConfiguration(Class clazz) {
        NodeEngine nodeEngine = getNodeEngine();
        return nodeEngine.getConfig().findCacheConfig(name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    // --------- NOT IMPLEMENTED -------------


    @Override
    public void loadAll(Set keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object invoke(Object key, EntryProcessor entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map invokeAll(Set keys, EntryProcessor entryProcessor, Object... arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheManager getCacheManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException();
    }
}
