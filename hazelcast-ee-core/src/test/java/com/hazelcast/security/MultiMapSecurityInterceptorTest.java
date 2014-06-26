package com.hazelcast.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.MultiMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.multimap.MultiMapService;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class MultiMapSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void put() {
        final String key = randomString();
        final String val = randomString();
        getMultiMap().put(key, val);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "put", key, val);
    }

    @Test
    public void get() {
        final String key = randomString();
        getMultiMap().get(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "get", key);
    }

    @Test
    public void test1_remove() {
        final String key = randomString();
        final String val = randomString();
        getMultiMap().remove(key, val);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "remove", key, val);
    }

    @Test
    public void test2_remove() {
        final String key = randomString();
        getMultiMap().remove(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "remove", key);
    }

    @Test
    public void keySet() {
        getMultiMap().keySet();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "keySet");
    }

    @Test
    public void values() {
        getMultiMap().values();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "values");
    }

    @Test
    public void entrySet() {
        getMultiMap().entrySet();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "entrySet");
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        getMultiMap().containsKey(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "containsKey", key);
    }

    @Test
    public void containsValue() {
        final String val = randomString();
        getMultiMap().containsValue(val);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "containsValue", val);
    }

    @Test
    public void containsEntry() {
        final String key = randomString();
        final String val = randomString();
        getMultiMap().containsEntry(key, val);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "containsEntry", key, val);
    }

    @Test
    public void size() {
        getMultiMap().size();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "size");
    }

    @Test
    public void clear() {
        getMultiMap().clear();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "clear");
    }

    @Test
    public void valueCount() {
        final String key = randomString();
        getMultiMap().valueCount(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "valueCount", key);
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        getMultiMap().addEntryListener(entryAdapter, true);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addEntryListener", null, true);
    }

    @Test
    public void test2_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String key = randomString();
        getMultiMap().addEntryListener(entryAdapter, key, false);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addEntryListener", null, key, false);
    }

    @Test
    public void test1_lock() {
        final String key = randomString();
        getMultiMap().lock(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "lock", key);
    }

    @Test
    public void test2_lock() {
        final String key = randomString();
        final long ttl = randomLong();
        getMultiMap().lock(key, ttl, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "lock", key, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isLocked() {
        final String key = randomString();
        getMultiMap().isLocked(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "isLocked", key);
    }

    @Test
    public void test1_tryLock() {
        final String key = randomString();
        getMultiMap().tryLock(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryLock", key);
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final String key = randomString();
        final long timeout = randomLong();
        getMultiMap().tryLock(key, timeout, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryLock", key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void unlock() {
        final String key = randomString();
        final MultiMap multiMap = getMultiMap();
        multiMap.lock(key);
        interceptor.reset();

        multiMap.unlock(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "unlock", key);
    }

    @Test
    public void forceUnlock() {
        final String key = randomString();
        final MultiMap multiMap = getMultiMap();
        multiMap.lock(key);
        interceptor.reset();

        multiMap.forceUnlock(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "forceUnlock", key);
    }

    MultiMap getMultiMap() {
        return client.getMultiMap(randomString());
    }

    @Override
    String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }
}
