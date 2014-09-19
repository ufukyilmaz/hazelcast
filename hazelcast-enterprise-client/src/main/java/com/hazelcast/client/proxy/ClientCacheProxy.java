//package com.hazelcast.client.proxy;
//
//import com.hazelcast.cache.CacheService;
//import com.hazelcast.cache.CacheStats;
//import com.hazelcast.cache.CacheStatsImpl;
//import com.hazelcast.cache.ICache;
//import com.hazelcast.cache.client.CacheAddInvalidationListenerRequest;
//import com.hazelcast.cache.client.CacheClearRequest;
//import com.hazelcast.cache.client.CacheContainsKeyRequest;
//import com.hazelcast.cache.client.CacheGetAndRemoveRequest;
//import com.hazelcast.cache.client.CacheGetAndReplaceRequest;
//import com.hazelcast.cache.client.CacheGetRequest;
//import com.hazelcast.cache.client.CacheInvalidationMessage;
//import com.hazelcast.cache.client.CachePutIfAbsentRequest;
//import com.hazelcast.cache.client.CachePutRequest;
//import com.hazelcast.cache.client.CacheRemoveRequest;
//import com.hazelcast.cache.client.CacheReplaceRequest;
//import com.hazelcast.cache.client.CacheSizeRequest;
//import com.hazelcast.client.cache.ClientCacheIterator;
//import com.hazelcast.client.impl.client.ClientRequest;
//import com.hazelcast.client.nearcache.ClientHeapNearCache;
//import com.hazelcast.client.nearcache.ClientNearCache;
//import com.hazelcast.client.nearcache.ClientOffHeapNearCache;
//import com.hazelcast.client.spi.ClientClusterService;
//import com.hazelcast.client.spi.ClientContext;
//import com.hazelcast.client.spi.ClientProxy;
//import com.hazelcast.client.spi.EventHandler;
//import com.hazelcast.client.spi.impl.ClientCallFuture;
//import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
//import com.hazelcast.config.InMemoryFormat;
//import com.hazelcast.config.NearCacheConfig;
//import com.hazelcast.core.Client;
//import com.hazelcast.core.ExecutionCallback;
//import com.hazelcast.core.ICompletableFuture;
//import com.hazelcast.core.Member;
//import com.hazelcast.core.MemberAttributeEvent;
//import com.hazelcast.core.MembershipEvent;
//import com.hazelcast.core.MembershipListener;
//import com.hazelcast.instance.MemberImpl;
//import com.hazelcast.nio.serialization.Data;
//import com.hazelcast.util.Clock;
//import com.hazelcast.util.ExceptionUtil;
//import com.hazelcast.util.executor.CompletedFuture;
//import com.hazelcast.util.executor.DelegatingFuture;
//
//import javax.cache.CacheManager;
//import javax.cache.configuration.CacheEntryListenerConfiguration;
//import javax.cache.configuration.Configuration;
//import javax.cache.integration.CompletionListener;
//import javax.cache.processor.EntryProcessorException;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
///**
// * @author enesakar 2/11/14
// */
//public final class ClientCacheProxy<K, V> extends ClientProxy implements ICache<K, V> {
//
//    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
//    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
//    private static final int MAX_CONSECUTIVE_ASYNC_COUNT = 100;
//    private static final long CONSECUTIVE_ASYNC_INTERVAL = 1000;
//
//    private final String name;
//    private final ClientNearCache<Data, Object> nearCache;
//    private final CacheStatsImpl cacheStats;
//    private final boolean cacheOnUpdate;
//    private final AtomicInteger asyncCount = new AtomicInteger();
//    private volatile long lastAsyncTime;
//    private volatile String membershipRegistrationId;
//
//    public ClientCacheProxy(String name, ClientContext context) {
//        super(CacheService.SERVICE_NAME, name);
//        this.name = name;
//        NearCacheConfig config = context.getClientConfig().getNearCacheConfig(name);
//        if (config != null) {
//            if (config.getInMemoryFormat() == InMemoryFormat.OFFHEAP) {
//                nearCache = new ClientOffHeapNearCache<Object>(name, context);
//            } else {
//                nearCache = new ClientHeapNearCache<Data>(name, context, config);
//            }
//            cacheOnUpdate = config.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
//        } else {
//            nearCache = null;
//            cacheOnUpdate = false;
//        }
//        cacheStats = new CacheStatsImpl(System.currentTimeMillis());
//    }
//
//    @Override
//    protected void onInitialize() {
//        registerInvalidationListener();
//    }
//
//    @Override
//    protected void onDestroy() {
//        destroyNearCache();
//    }
//
//    private void destroyNearCache() {
//        if (nearCache != null) {
//            removeInvalidationListener();
//            nearCache.destroy();
//        }
//    }
//
//    @Override
//    protected void onShutdown() {
//        destroyNearCache();
//    }
//
//    @Override
//    public String toString() {
//        return "Cache{" + "name='" + getName() + '\'' + '}';
//    }
//
//    @Override
//    public V get(K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        Object cached = nearCache != null ? nearCache.get(keyData) : null;
//        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
//            return (V) cached;
//        }
//        CacheGetRequest request = new CacheGetRequest(name, keyData);
//        try {
//            ClientContext context = getContext();
//            Future future = context.getInvocationService().invokeOnKeyOwner(request, keyData);
//            Data valueData = (Data) future.get();
//
//            V value = context.getSerializationService().toObject(valueData);
//            storeInNearCache(keyData, valueData, value);
//            if (value != null) {
//                cacheStats.updateGetStats(true, start);
//            } else {
//                cacheStats.updateGetStats(false, start);
//            }
//            return value;
//
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
//    }
//
//
//    private void storeInNearCache(Data key, Data valueData, V value) {
//        if (nearCache != null && valueData != null) {
//            if (nearCache.getInMemoryFormat() == InMemoryFormat.BINARY
//                    || nearCache.getInMemoryFormat() == InMemoryFormat.OFFHEAP) {
//                nearCache.put(key, valueData);
//            } else {
//                if (value == null) {
//                    value = getContext().getSerializationService().toObject(valueData);
//                }
//                nearCache.put(key, value);
//            }
//        }
//    }
//
//    private void invalidateNearCache(Data key) {
//        if (nearCache != null) {
//            nearCache.remove(key);
//        }
//    }
//
//    @Override
//    public boolean containsKey(K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        final Data keyData = toData(key);
//        Object cached = nearCache != null ? nearCache.get(keyData) : null;
//        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
//            return true;
//        }
//        CacheContainsKeyRequest request = new CacheContainsKeyRequest(name, keyData);
//        Object result = invoke(request, keyData);
//        cacheStats.setLastAccessTime(System.currentTimeMillis());
//        return (Boolean) result;
//    }
//
//    @Override
//    public void put(K key, V value) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CachePutRequest request = new CachePutRequest(name, keyData, valueData, -1);
//        invoke(request, keyData);
//        if (cacheOnUpdate) {
//            storeInNearCache(keyData, valueData, value);
//        } else {
//            invalidateNearCache(keyData);
//        }
//        cacheStats.updatePutStats(start);
//    }
//
//    @Override
//    public void put(K key, V value, long ttl, TimeUnit timeUnit) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CachePutRequest request = new CachePutRequest(name, keyData, valueData, timeUnit.toMillis(ttl));
//        invoke(request, keyData);
//        if (cacheOnUpdate) {
//            storeInNearCache(keyData, valueData, value);
//        } else {
//            invalidateNearCache(keyData);
//        }
//        cacheStats.updatePutStats(start);
//    }
//
//    @Override
//    public V getAndPut(K key, V v) {
//        return getAndPut(key, v, -1, TimeUnit.SECONDS);
//    }
//
//    @Override
//    public V getAndPut(K key, V value, long ttl, TimeUnit timeUnit) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CachePutRequest request = new CachePutRequest(name, keyData, valueData, timeUnit.toMillis(ttl), true);
//        V currentValue = invoke(request, keyData);
//        if (cacheOnUpdate) {
//            storeInNearCache(keyData, valueData, value);
//        } else {
//            invalidateNearCache(keyData);
//        }
//        cacheStats.updatePutStats(start);
//        return currentValue;
//    }
//
//    @Override
//    public boolean remove(K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        CacheRemoveRequest request = new CacheRemoveRequest(name, keyData);
//        Boolean removed = invoke(request, keyData);
//        if (removed == null) {
//            return false;
//        }
//        if (removed) {
//            invalidateNearCache(keyData);
//        }
//        cacheStats.updateRemoveStats(start);
//        return removed;
//    }
//
//    @Override
//    public boolean remove(K key, V value) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CacheRemoveRequest request = new CacheRemoveRequest(name, keyData, valueData);
//        Boolean removed = invoke(request, keyData);
//        if (removed == null) {
//            return false;
//        }
//        if (removed) {
//            invalidateNearCache(keyData);
//        }
//        cacheStats.updateRemoveStats(start);
//        return removed;
//    }
//
//    @Override
//    public V getAndRemove(K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        CacheGetAndRemoveRequest request = new CacheGetAndRemoveRequest(name, keyData);
//        V value = invoke(request, keyData);
//        invalidateNearCache(keyData);
//        cacheStats.updateRemoveStats(start);
//        return value;
//    }
//
//    @Override
//    public boolean putIfAbsent(K key, V value) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(name, keyData, valueData);
//        Boolean put = invoke(request, keyData);
//        if (put == null) {
//            return false;
//        }
//        if (put) {
//            if (cacheOnUpdate) {
//                storeInNearCache(keyData, valueData, value);
//            } else {
//                invalidateNearCache(keyData);
//            }
//        }
//        cacheStats.updatePutStats(start);
//        return put;
//    }
//
//    @Override
//    public boolean replace(K key, V value) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CacheReplaceRequest request = new CacheReplaceRequest(name, keyData, valueData);
//        Boolean replaced = invoke(request, keyData);
//        if (replaced == null) {
//            return false;
//        }
//        if (replaced) {
//            if (cacheOnUpdate) {
//                storeInNearCache(keyData, valueData, value);
//            } else {
//                invalidateNearCache(keyData);
//            }
//        }
//        cacheStats.updatePutStats(start);
//        return replaced;
//    }
//
//    @Override
//    public boolean replace(K key, V currentValue, V value) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data currentValueData = toData(currentValue);
//        final Data valueData = toData(value);
//        CacheReplaceRequest request = new CacheReplaceRequest(name, keyData, currentValueData, valueData);
//        Boolean replaced = invoke(request, keyData);
//        if (replaced == null) {
//            return false;
//        }
//        if (replaced) {
//            if (cacheOnUpdate) {
//                storeInNearCache(keyData, valueData, value);
//            } else {
//                invalidateNearCache(keyData);
//            }
//        }
//        cacheStats.updatePutStats(start);
//        return replaced;
//    }
//
//    @Override
//    public V getAndReplace(K key, V value) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        long start = System.currentTimeMillis();
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CacheGetAndReplaceRequest request = new CacheGetAndReplaceRequest(name, keyData, valueData);
//        V currentValue = invoke(request, keyData);
//        if (currentValue != null) {
//            if (cacheOnUpdate) {
//                storeInNearCache(keyData, valueData, value);
//            } else {
//                invalidateNearCache(keyData);
//            }
//        }
//        cacheStats.updatePutStats(start);
//        return currentValue;
//    }
//
//    @Override
//    public int size() {
//        CacheSizeRequest request = new CacheSizeRequest(name);
//        Integer result = invoke(request);
//        if (result == null) {
//            return 0;
//        }
//        return result;
//    }
//
//    @Override
//    public Iterator<Entry<K, V>> iterator() {
//        return iterator(100);
//    }
//
//    @Override
//    public Iterator<Entry<K, V>> iterator(int fetchCount) {
//        return new ClientCacheIterator<K, V>(this, fetchCount, getContext());
//    }
//
//    @Override
//    public CacheStats getStats() {
//        if (nearCache != null) {
//            cacheStats.setNearCacheStats(nearCache.getNearCacheStats());
//        }
//        return cacheStats;
//    }
//
//    @Override
//    public void removeAll() {
//        clear();
//    }
//
//    @Override
//    public void clear() {
//        CacheClearRequest request = new CacheClearRequest(name);
//        invoke(request);
//        if (nearCache != null) {
//            nearCache.clear();
//        }
//    }
//
//    private boolean shouldBeSync() {
//        boolean sync = false;
//        long last = lastAsyncTime;
//        long now = Clock.currentTimeMillis();
//        if (last + CONSECUTIVE_ASYNC_INTERVAL < now) {
//            asyncCount.set(0);
//        } else if (asyncCount.incrementAndGet() >= MAX_CONSECUTIVE_ASYNC_COUNT) {
//            asyncCount.set(0);
//            sync = true;
//        }
//        lastAsyncTime = now;
//        return sync;
//    }
//
//    @Override
//    public Future<V> getAsync(final K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (shouldBeSync()) {
//            V value = get(key);
//            return createCompletedFuture(value);
//        }
//        final Data keyData = toData(key);
//        Object cached = nearCache != null ? nearCache.get(keyData) : null;
//        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
//            return createCompletedFuture(cached);
//        }
//
//        CacheGetRequest request = new CacheGetRequest(name, keyData);
//        ClientCallFuture future;
//        final ClientContext context = getContext();
//        try {
//            future = (ClientCallFuture) context.getInvocationService().invokeOnKeyOwner(request, keyData);
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
//        if (nearCache != null) {
//            future.andThenInternal(new ExecutionCallback<Data>() {
//                public void onResponse(Data valueData) {
//                    storeInNearCache(keyData, valueData, null);
//                }
//
//                public void onFailure(Throwable t) {
//                }
//            });
//        }
//        cacheStats.updateGetStats();
//        return new DelegatingFuture<V>(future, getContext().getSerializationService());
//    }
//
//    private Future createCompletedFuture(Object value) {
//        return new CompletedFuture(getContext().getSerializationService(), value,
//                getContext().getExecutionService().getAsyncExecutor());
//    }
//
//    @Override
//    public Future<Void> putAsync(K key, V value) {
//        return putAsync(key, value, -1, TimeUnit.SECONDS);
//    }
//
//    @Override
//    public Future<Void> putAsync(final K key, final V value, long ttl, TimeUnit timeUnit) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        if (shouldBeSync()) {
//            put(key, value, ttl, timeUnit);
//            return createCompletedFuture(null);
//        }
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CachePutRequest request = new CachePutRequest(name, keyData, valueData, timeUnit.toMillis(ttl), false);
//        ICompletableFuture future;
//        try {
//            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
//            if (cacheOnUpdate) {
//                storeInNearCache(keyData, valueData, value);
//            } else {
//                invalidateNearCache(keyData);
//            }
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
////        if (nearCache != null) {
////            future.andThen(new ExecutionCallback<Void>() {
////                public void onResponse(Void response) {
////                    if (cacheOnUpdate) {
////                        storeInNearCache(keyData, valueData, value);
////                    } else {
////                        invalidateNearCache(keyData);
////                    }
////                }
////
////                public void onFailure(Throwable t) {
////                }
////            });
////        }
//        cacheStats.updatePutStats();
//        return new DelegatingFuture<Void>(future, getContext().getSerializationService());
//    }
//
//    @Override
//    public Future<V> getAndPutAsync(K key, V value) {
//        return getAndPutAsync(key, value, -1, TimeUnit.SECONDS);
//    }
//
//    @Override
//    public Future<V> getAndPutAsync(final K key, final V value, long ttl, TimeUnit timeUnit) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (value == null) {
//            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
//        }
//        if (shouldBeSync()) {
//            V oldValue = getAndPut(key, value, ttl, timeUnit);
//            return createCompletedFuture(oldValue);
//        }
//        final Data keyData = toData(key);
//        final Data valueData = toData(value);
//        CachePutRequest request = new CachePutRequest(name, keyData, valueData, timeUnit.toMillis(ttl), true);
//        ICompletableFuture future;
//        try {
//            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
//            if (cacheOnUpdate) {
//                storeInNearCache(keyData, valueData, value);
//            } else {
//                invalidateNearCache(keyData);
//            }
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
////        if (nearCache != null) {
////            future.andThen(new ExecutionCallback() {
////                public void onResponse(Object response) {
////                    if (cacheOnUpdate) {
////                        storeInNearCache(keyData, valueData, value);
////                    } else {
////                        invalidateNearCache(keyData);
////                    }
////                }
////
////                public void onFailure(Throwable t) {
////                }
////            });
////        }
//        cacheStats.updatePutStats();
//        return new DelegatingFuture<V>(future, getContext().getSerializationService());
//    }
//
//
//    @Override
//    public Future<Boolean> removeAsync(final K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (shouldBeSync()) {
//            remove(key);
//            return createCompletedFuture(null);
//        }
//        final Data keyData = toData(key);
//        CacheRemoveRequest request = new CacheRemoveRequest(name, keyData);
//        ICompletableFuture future;
//        try {
//            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
//            invalidateNearCache(keyData);
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
////        if (nearCache != null) {
////            future.andThen(new ExecutionCallback<Boolean>() {
////                public void onResponse(Boolean response) {
////                    invalidateNearCache(keyData);
////                }
////
////                public void onFailure(Throwable t) {
////                }
////            });
////        }
//        cacheStats.updateRemoveStats();
//        return new DelegatingFuture<Boolean>(future, getContext().getSerializationService());
//    }
//
//    @Override
//    public Future<V> getAndRemoveAsync(final K key) {
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        if (shouldBeSync()) {
//            V value = getAndRemove(key);
//            return createCompletedFuture(value);
//        }
//        final Data keyData = toData(key);
//        CacheGetAndRemoveRequest request = new CacheGetAndRemoveRequest(name, keyData);
//        ICompletableFuture future;
//        try {
//            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
//            invalidateNearCache(keyData);
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
////        if (nearCache != null) {
////            future.andThen(new ExecutionCallback() {
////                public void onResponse(Object response) {
////                    invalidateNearCache(keyData);
////                }
////
////                public void onFailure(Throwable t) {
////                }
////            });
////        }
//        cacheStats.updateRemoveStats();
//        return new DelegatingFuture<V>(future, getContext().getSerializationService());
//    }
//
//    @Override
//    public void close() {
//        destroy();
//    }
//
//    @Override
//    public Object unwrap(Class clazz) {
//        if (clazz.equals(ICache.class)) {
//            return this;
//        }
//        throw new IllegalArgumentException();
//    }
//
//    @Override
//    public Map<K, V> getAll(Set<? extends K> ks) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public void loadAll(Set<? extends K> ks, boolean b, CompletionListener completionListener) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public void putAll(Map<? extends K, ? extends V> map) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public void removeAll(Set<? extends K> ks) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public Configuration<K, V> getConfiguration() {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public <T> T invoke(K key, javax.cache.processor.EntryProcessor<K, V, T> kvtEntryProcessor, Object... objects)
//            throws EntryProcessorException {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public <T> Map<K, T> invokeAll(Set<? extends K> ks, javax.cache.processor.EntryProcessor<K, V, T> kvtEntryProcessor,
//                                   Object... objects) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public CacheManager getCacheManager() {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public boolean isClosed() {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> kvCacheEntryListenerConfiguration) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    @Override
//    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> kvCacheEntryListenerConfiguration) {
//        throw new UnsupportedOperationException("This operation is not supported for client.");
//    }
//
//    private void registerInvalidationListener() {
//        if (nearCache != null && nearCache.isInvalidateOnChange()) {
//            ClientClusterService clusterService = getContext().getClusterService();
//            membershipRegistrationId = clusterService.addMembershipListener(new NearCacheMembershipListener());
//
//            Collection<MemberImpl> memberList = clusterService.getMemberList();
//            for (MemberImpl member : memberList) {
//                addInvalidationListener(member);
//            }
//        }
//    }
//
//    private void removeInvalidationListener() {
//        if (nearCache != null && nearCache.isInvalidateOnChange()) {
//            String registrationId = membershipRegistrationId;
//            ClientClusterService clusterService = getContext().getClusterService();
//            if (registrationId != null) {
//                clusterService.removeMembershipListener(registrationId);
//            }
//            Collection<MemberImpl> memberList = clusterService.getMemberList();
//            for (MemberImpl member : memberList) {
//                removeInvalidationListener(member);
//            }
//        }
//    }
//
//    private class NearCacheInvalidationHandler implements EventHandler<CacheInvalidationMessage> {
//
//        final Client client;
//
//        private NearCacheInvalidationHandler(Client client) {
//            this.client = client;
//        }
//
//        @Override
//        public void handle(CacheInvalidationMessage message) {
//            if (client.getUuid().equals(message.getSourceUuid())) {
//                return;
//            }
//            Data key = message.getKey();
//            if (key != null) {
//                nearCache.remove(key);
//            } else {
//                nearCache.clear();
//            }
//        }
//
//        @Override
//        public void beforeListenerRegister() {
//            nearCache.clear();
//        }
//
//        @Override
//        public void onListenerRegister() {
//            nearCache.clear();
//        }
//    }
//
//    private final ConcurrentMap<Member, String> listeners = new ConcurrentHashMap<Member, String>();
//
//    private class NearCacheMembershipListener implements MembershipListener {
//
//        public void memberAdded(MembershipEvent event) {
//            MemberImpl member = (MemberImpl) event.getMember();
//            addInvalidationListener(member);
//
//        }
//
//        public void memberRemoved(MembershipEvent event) {
//            MemberImpl member = (MemberImpl) event.getMember();
//            removeInvalidationListener(member);
//        }
//
//        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
//        }
//    }
//
//    //TODO come up with a better sync
//    private synchronized void addInvalidationListener(MemberImpl member) {
//        if (listeners.containsKey(member)) {
//            return;
//        }
//        try {
//            ClientContext context = getContext();
//            ClientRequest request = new CacheAddInvalidationListenerRequest(name);
//            Client client = context.getClusterService().getLocalClient();
//            EventHandler handler = new NearCacheInvalidationHandler(client);
//            ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) context.getInvocationService();
//            Future future = invocationService.invokeOnTarget(request, member.getAddress(), handler);
//            String registrationId = context.getSerializationService().toObject(future.get());
//            context.getListenerService().registerListener(registrationId, request.getCallId());
//            listeners.put(member, registrationId);
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
//    }
//
//    private void removeInvalidationListener(MemberImpl member) {
//        String registrationId = listeners.remove(member);
//        if (registrationId != null) {
//            try {
//                ClientContext context = getContext();
//                context.getListenerService().deRegisterListener(registrationId);
//            } catch (Exception e) {
//                throw ExceptionUtil.rethrow(e);
//            }
//        }
//    }
//
//}
