package com.hazelcast.client.security;

import com.hazelcast.cache.CacheContextTest;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.quorum.HiDensityClientCacheReadWriteQuorumTest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import java.util.HashMap;
import java.util.HashSet;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    ICache cache;

    @Before
    public void setup() {

        HazelcastClientCachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        objectName = randomString();
        CacheConfig cacheConfig = new CacheConfig();
        cache = (ICache) cachingProvider.getCacheManager().createCache(objectName, cacheConfig);
    }

    @Override
    String getObjectType() {
        return CacheService.SERVICE_NAME;
    }

    String getNameWithPrefix() {
        return "/hz/" + objectName;
    }

    @Test
    public void get() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "get", key);
        cache.get(key);
    }

    @Test
    public void getAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "getAll", keys);
        cache.getAll(keys);
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "containsKey", key);
        cache.containsKey(key);
    }

    @Ignore
    @Test
    public void loadAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());

        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "loadAll", keys, true, null);
        cache.loadAll(keys, true, null);
    }

    @Test
    public void put() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "put", key, val);
        cache.put(key, val);
    }

    @Test
    public void getAndPut() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "getAndPut", key, val);
        cache.getAndPut(key, val);
    }

    @Ignore
    @Test
    public void putAll() {
        final HashMap key = new HashMap();
        key.put(randomString(), randomString());
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "putAll", key);
        cache.putAll(key);
    }

    @Test
    public void putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "putIfAbsent", key, val);
        cache.putIfAbsent(key, val);
    }

    @Test
    public void remove() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "remove", key);
        cache.remove(key);
    }

    @Test
    public void remove_WithOldValue() {
        final String key = randomString();
        final String oldVal = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "remove", key, oldVal);
        cache.remove(key, oldVal);
    }

    @Test
    public void getAndRemove() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "getAndRemove", key);
        cache.getAndRemove(key);
    }

    @Test
    public void replace() {
        final String key = randomString();
        final String oldVal = randomString();
        final String newVal = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "replace", key, oldVal, newVal);
        cache.replace(key, oldVal, newVal);
    }

    @Test
    public void replace_WithOutNewValue() {
        final String key = randomString();
        final String value = randomString();
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "replace", key, value);
        cache.replace(key, value);
    }

    @Test
    public void replace_withExpiryPolicy(){
        final String key = randomString();
        final String oldVal = randomString();
        final String newVal = randomString();
        HazelcastExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1,1,1);

        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "replace", key, oldVal, newVal, expiryPolicy);
        cache.replace(key, oldVal, newVal, expiryPolicy);
    }

    @Test
    public void replace_withoutNewValueWithExpiryPolicy() {
        final String key = randomString();
        final String value = randomString();
        HazelcastExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1,1,1);

        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "replace", key, value, expiryPolicy);
        cache.replace(key, value, expiryPolicy);
    }

    @Test
    public void removeAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "removeAll", keys);
        cache.removeAll(keys);
    }

    @Test
    public void removeAll_WithOutKey() {
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "removeAll");
        cache.removeAll();
    }

    @Test
    public void clear() {
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "clear");
        cache.clear();
    }

    @Ignore
    @Test
    public void getConfiguration() {
        // it does not make a remote call, no need for security check
    }

    @Ignore
    @Test
    public void invoke() {
        final String keys = randomString();

        final javax.cache.processor.EntryProcessor entryProcessor =
                new HiDensityClientCacheReadWriteQuorumTest.SimpleEntryProcessor();

        cache.invoke(keys, entryProcessor, null);
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "invoke", keys, null, null);
    }

    @Ignore
    @Test
    public void invokeAll() {
        HashSet keys = new HashSet();
        final javax.cache.processor.EntryProcessor entryProcessor =
                new HiDensityClientCacheReadWriteQuorumTest.SimpleEntryProcessor();

        keys.add(randomString());
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "invokeAll", keys, null, null);
        cache.invokeAll(keys, entryProcessor, null);
    }

    @Ignore
    @Test
    public void getName() {
        // it does not make a remote call, no need for security check
    }

    @Ignore
    @Test
    public void getCacheManager() {
        // it does not make a remote call, no need for security check
    }

    @Ignore
    @Test
    public void close() {
        // it does not make a remote call, no need for security check

    }

    @Ignore
    @Test
    public void isClosed() {
        // it does not make a remote call, no need for security check
    }

    @Ignore
    @Test
    public void unwrap() {
        // it does not make a remote call, no need for security check

    }

    @Test
    public void registerCacheEntryListener() {

        CacheEntryListenerConfiguration<String, String> config =
                new MutableCacheEntryListenerConfiguration<String, String>(
                        FactoryBuilder.factoryOf(new CacheContextTest.TestListener()), null, true, true);

        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "registerCacheEntryListener", null);
        cache.registerCacheEntryListener(config);
    }

    @Test
    public void deregisterCacheEntryListener() {
        CacheEntryListenerConfiguration<String, String> config =
                new MutableCacheEntryListenerConfiguration<String, String>(
                        FactoryBuilder.factoryOf(new CacheContextTest.TestListener()), null, true, true);

        cache.registerCacheEntryListener(config);
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "deregisterCacheEntryListener", null);
        cache.deregisterCacheEntryListener(config);
    }

    @Test
    public void iterator() {
        interceptor.setExpectation(getObjectType(), getNameWithPrefix(), "iterator");
        cache.iterator();
    }
}
