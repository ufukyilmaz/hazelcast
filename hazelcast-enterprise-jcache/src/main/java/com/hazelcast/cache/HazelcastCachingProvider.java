package com.hazelcast.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Properties;

public final class HazelcastCachingProvider implements CachingProvider {

    // TODO: add a system property to select cache provider to use
    private static final String CLIENT_CACHING_PROVIDER_CLASS = "com.hazelcast.client.cache.HazelcastClientCachingProvider";
    private static final ILogger logger = Logger.getLogger(HazelcastCachingProvider.class);

    private final CachingProvider delegate;

    public HazelcastCachingProvider() {
        CachingProvider cp = null;
        try {
            cp = ClassLoaderUtil.newInstance(getClass().getClassLoader(), CLIENT_CACHING_PROVIDER_CLASS);
        } catch (Exception e) {
            logger.warning("Could not load client CachingProvider! Fallback to server one... " + e.toString());
        }
        if (cp == null) {
            cp = new HazelcastServerCachingProvider();
        }
        delegate = cp;
    }

    public HazelcastCachingProvider(HazelcastInstance instance) {
        CachingProvider cp = null;
        if (instance instanceof HazelcastInstanceProxy || instance instanceof HazelcastInstanceImpl) {
            cp = new HazelcastServerCachingProvider(instance);
        } else {
            try {
                Class<?> clazz = ClassLoaderUtil.loadClass(getClass().getClassLoader(), CLIENT_CACHING_PROVIDER_CLASS);
                Constructor<?> constructor = clazz
                        .getDeclaredConstructor(new Class[] {HazelcastInstance.class});
                cp = (CachingProvider) constructor.newInstance(instance);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
        delegate = cp;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return delegate.getCacheManager(uri, classLoader, properties);
    }

    @Override
    public ClassLoader getDefaultClassLoader() {return delegate.getDefaultClassLoader();}

    @Override
    public URI getDefaultURI() {return delegate.getDefaultURI();}

    @Override
    public Properties getDefaultProperties() {return delegate.getDefaultProperties();}

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return delegate.getCacheManager(uri, classLoader);
    }

    @Override
    public CacheManager getCacheManager() {return delegate.getCacheManager();}

    @Override
    public void close() {delegate.close();}

    @Override
    public void close(ClassLoader classLoader) {delegate.close(classLoader);}

    @Override
    public void close(URI uri, ClassLoader classLoader) {delegate.close(uri, classLoader);}

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {return delegate.isSupported(optionalFeature);}


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastCachingProvider{");
        sb.append("delegate=").append(delegate);
        sb.append('}');
        return sb.toString();
    }
}
