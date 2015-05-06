package com.hazelcast.client.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.MultiMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.multimap.impl.MultiMapService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Ignore
public class MultiMapSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    MultiMap multiMap;

    @Before
    public void setup() {
        objectName = randomString();
        multiMap = client.getMultiMap(objectName);
    }

    @Test
    public void put() {
        final String key = randomString();
        final String val = randomString();
        multiMap.put(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "put", key, val);
    }

    @Test
    public void get() {
        final String key = randomString();
        multiMap.get(key);
        interceptor.assertMethod(getObjectType(), objectName, "get", key);
    }

    @Test
    public void test1_remove() {
        final String key = randomString();
        final String val = randomString();
        multiMap.remove(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "remove", key, val);
    }

    @Test
    public void test2_remove() {
        final String key = randomString();
        multiMap.remove(key);
        interceptor.assertMethod(getObjectType(), objectName, "remove", key);
    }

    @Test
    public void keySet() {
        multiMap.keySet();
        interceptor.assertMethod(getObjectType(), objectName, "keySet");
    }

    @Test
    public void values() {
        multiMap.values();
        interceptor.assertMethod(getObjectType(), objectName, "values");
    }

    @Test
    public void entrySet() {
        multiMap.entrySet();
        interceptor.assertMethod(getObjectType(), objectName, "entrySet");
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        multiMap.containsKey(key);
        interceptor.assertMethod(getObjectType(), objectName, "containsKey", key);
    }

    @Test
    public void containsValue() {
        final String val = randomString();
        multiMap.containsValue(val);
        interceptor.assertMethod(getObjectType(), objectName, "containsValue", val);
    }

    @Test
    public void containsEntry() {
        final String key = randomString();
        final String val = randomString();
        multiMap.containsEntry(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "containsEntry", key, val);
    }

    @Test
    public void size() {
        multiMap.size();
        interceptor.assertMethod(getObjectType(), objectName, "size");
    }

    @Test
    public void clear() {
        multiMap.clear();
        interceptor.assertMethod(getObjectType(), objectName, "clear");
    }

    @Test
    public void valueCount() {
        final String key = randomString();
        multiMap.valueCount(key);
        interceptor.assertMethod(getObjectType(), objectName, "valueCount", key);
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        multiMap.addEntryListener(entryAdapter, true);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, true);
    }

    @Test
    public void test2_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String key = randomString();
        multiMap.addEntryListener(entryAdapter, key, false);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, key, false);
    }

    @Test
    public void test1_lock() {
        final String key = randomString();
        multiMap.lock(key);
        interceptor.assertMethod(getObjectType(), objectName, "lock", key);
    }

    @Test
    public void test2_lock() {
        final String key = randomString();
        final long ttl = randomLong() + 1;
        multiMap.lock(key, ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "lock", key, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isLocked() {
        final String key = randomString();
        multiMap.isLocked(key);
        interceptor.assertMethod(getObjectType(), objectName, "isLocked", key);
    }

    @Test
    public void test1_tryLock() {
        final String key = randomString();
        multiMap.tryLock(key);
        interceptor.assertMethod(getObjectType(), objectName, "tryLock", key);
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final String key = randomString();
        final long timeout = randomLong() + 1;
        multiMap.tryLock(key, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryLock", key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void unlock() {
        final String key = randomString();
        multiMap.lock(key);
        interceptor.reset();

        multiMap.unlock(key);
        interceptor.assertMethod(getObjectType(), objectName, "unlock", key);
    }

    @Test
    public void forceUnlock() {
        final String key = randomString();
        multiMap.lock(key);
        interceptor.reset();

        multiMap.forceUnlock(key);
        interceptor.assertMethod(getObjectType(), objectName, "forceUnlock", key);
    }

    @Override
    String getObjectType() {
        return MultiMapService.SERVICE_NAME;
    }
}
