//package com.hazelcast.client.cache;
//
//import com.hazelcast.cache.HazelcastCacheManager;
//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.client.config.XmlClientConfigBuilder;
//import com.hazelcast.core.HazelcastInstance;
//
//import javax.cache.CacheException;
//import javax.cache.configuration.OptionalFeature;
//import javax.cache.spi.CachingProvider;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.Properties;
//
//public final class HazelcastClientCachingProvider implements CachingProvider {
//
//    protected final HazelcastInstance hazelcastInstance;
//    protected final HazelcastCacheManager cacheManager;
//
//    public HazelcastClientCachingProvider() {
//        ClientConfig config = new XmlClientConfigBuilder().build();
//        hazelcastInstance = HazelcastClient.newHazelcastClient(config);
//        cacheManager = new HazelcastClientCacheManager(this);
//    }
//
//    public HazelcastClientCachingProvider(HazelcastInstance hazelcastInstance) {
//        this.hazelcastInstance = hazelcastInstance;
//        cacheManager = new HazelcastClientCacheManager(this);
//    }
//
//    @Override
//    public HazelcastCacheManager getCacheManager() {
//        return cacheManager;
//    }
//
//    @Override
//    public HazelcastCacheManager getCacheManager(URI uri, ClassLoader classLoader) {
//        return getCacheManager();
//    }
//
//    @Override
//    public HazelcastCacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
//        return getCacheManager();
//    }
//
//    @Override
//    public ClassLoader getDefaultClassLoader() {
//        return getClass().getClassLoader();
//    }
//
//    @Override
//    public URI getDefaultURI() {
//        try {
//            return new URI(this.getClass().getName());
//        } catch (URISyntaxException e) {
//            throw new CacheException("Cannot create Default URI",e);
//        }
//    }
//
//    @Override
//    public Properties getDefaultProperties() {
//        return null;
//    }
//
//    @Override
//    public void close() {
//        hazelcastInstance.shutdown();
//    }
//
//    @Override
//    public void close(ClassLoader classLoader) {
//        close();
//    }
//
//    @Override
//    public void close(URI uri, ClassLoader classLoader) {
//        close();
//    }
//
//    @Override
//    public boolean isSupported(OptionalFeature optionalFeature) {
//        return false;
//    }
//
//    @Override
//    public String toString() {
//        final StringBuilder sb = new StringBuilder("HazelcastClientCachingProvider{");
//        sb.append("hazelcastInstance=").append(hazelcastInstance);
//        sb.append('}');
//        return sb.toString();
//    }
//}
