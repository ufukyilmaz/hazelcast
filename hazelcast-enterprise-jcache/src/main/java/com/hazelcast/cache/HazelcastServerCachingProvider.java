package com.hazelcast.cache;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.CacheException;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public final class HazelcastServerCachingProvider implements CachingProvider {

    protected final HazelcastInstance hazelcastInstance;
    protected final HazelcastCacheManager cacheManager;

    public HazelcastServerCachingProvider() {
        Config config = new XmlConfigBuilder().build();
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        cacheManager = new HazelcastServerCacheManager(this);
    }

    public HazelcastServerCachingProvider(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        cacheManager = new HazelcastServerCacheManager(this);
    }

    @Override
    public HazelcastCacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public HazelcastCacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager();
    }

    @Override
    public HazelcastCacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return getCacheManager();
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        try {
            return new URI(getClass().getName());
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create Default URI",e);
        }
    }

    @Override
    public Properties getDefaultProperties() {
        return null;
    }

    @Override
    public void close() {
        hazelcastInstance.shutdown();
    }

    @Override
    public void close(ClassLoader classLoader) {
        close();
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        close();
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastServerCachingProvider{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append('}');
        return sb.toString();
    }
}
