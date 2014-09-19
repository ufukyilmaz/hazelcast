package com.hazelcast.cache;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HazelcastCacheManager implements CacheManager {

    protected final HazelcastInstance hazelcastInstance;
    protected final CachingProvider cachingProvider;
    protected final Set<String> caches = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    protected HazelcastCacheManager(HazelcastInstance hazelcastInstance, CachingProvider cachingProvider) {
        this.hazelcastInstance = hazelcastInstance;
        this.cachingProvider = cachingProvider;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return cachingProvider;
    }

    @Override
    public URI getURI() {
        return cachingProvider.getDefaultURI();
    }

    @Override
    public Properties getProperties() {
        return cachingProvider.getDefaultProperties();
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return getCache(cacheName);
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        ICache<K, V> cache = hazelcastInstance
                .getDistributedObject(CacheService.SERVICE_NAME, cacheName);
        caches.add(cacheName);
        return cache;
    }

    @Override
    public Iterable<String> getCacheNames() {
        return caches;
    }

    @Override
    public void destroyCache(String cacheName) {
        boolean removed = caches.remove(cacheName);
        if (removed) {
            HazelcastInstance hz = hazelcastInstance;
            DistributedObject cache = hz.getDistributedObject(CacheService.SERVICE_NAME, cacheName);
            cache.destroy();
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        HazelcastInstance hz = hazelcastInstance;
        if (!hz.getLifecycleService().isRunning()) {
            return;
        }
        try {
            Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
            for (DistributedObject distributedObject : distributedObjects) {
                if (distributedObject instanceof ICache) {
                    try {
                        distributedObject.destroy();
                    } catch (DistributedObjectDestroyedException ignored) {
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ignored) {
        }
    }

    @Override
    public boolean isClosed() {
        return !hazelcastInstance.getLifecycleService().isRunning();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(HazelcastCacheManager.class)) {
            return (T) this;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public ClassLoader getClassLoader() {
        return HazelcastCacheManager.class.getClassLoader();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastCacheManager{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append(", cachingProvider=").append(cachingProvider);
        sb.append('}');
        return sb.toString();
    }
}
