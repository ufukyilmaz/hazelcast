package com.hazelcast.client.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.MultiMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.multimap.impl.MultiMapService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
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
        interceptor.setExpectation(getObjectType(), objectName, "put", key, val);
        multiMap.put(key, val);
    }

    @Test
    public void get() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "get", key);
        multiMap.get(key);
    }

    @Test
    public void test1_remove() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "remove", key, val);
        multiMap.remove(key, val);
    }

    @Test
    public void test2_remove() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "remove", key);
        multiMap.remove(key);
    }

    @Test
    public void keySet() {
        interceptor.setExpectation(getObjectType(), objectName, "keySet");
        multiMap.keySet();
    }

    @Test
    public void values() {
        interceptor.setExpectation(getObjectType(), objectName, "values");
        multiMap.values();
    }

    @Test
    public void entrySet() {
        interceptor.setExpectation(getObjectType(), objectName, "entrySet");
        multiMap.entrySet();
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsKey", key);
        multiMap.containsKey(key);
    }

    @Test
    public void containsValue() {
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsValue", val);
        multiMap.containsValue(val);
    }

    @Test
    public void containsEntry() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsEntry", key, val);
        multiMap.containsEntry(key, val);
    }

    @Test
    public void size() {
        interceptor.setExpectation(getObjectType(), objectName, "size");
        multiMap.size();
    }

    @Test
    public void clear() {
        interceptor.setExpectation(getObjectType(), objectName, "clear");
        multiMap.clear();
    }

    @Test
    public void valueCount() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "valueCount", key);
        multiMap.valueCount(key);
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, true);
        multiMap.addEntryListener(entryAdapter, true);
    }

    @Test
    public void test2_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, key, false);
        multiMap.addEntryListener(entryAdapter, key, false);
    }

    @Test
    public void test1_lock() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "lock", key);
        multiMap.lock(key);
    }

    @Test
    public void test2_lock() {
        final String key = randomString();
        final long ttl = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "lock", key, ttl, TimeUnit.MILLISECONDS);
        multiMap.lock(key, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isLocked() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "isLocked", key);
        multiMap.isLocked(key);
    }

    @Test
    public void test1_tryLock() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", key);
        multiMap.tryLock(key);
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final String key = randomString();
        final long timeout = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", key, timeout, TimeUnit.MILLISECONDS);
        multiMap.tryLock(key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void unlock() {
        final String key = randomString();
        multiMap.lock(key);

        interceptor.setExpectation(getObjectType(), objectName, "unlock", key);
        multiMap.unlock(key);
    }

    @Test
    public void forceUnlock() {
        final String key = randomString();
        multiMap.lock(key);
        interceptor.setExpectation(getObjectType(), objectName, "forceUnlock", key);
        multiMap.forceUnlock(key);
    }

    @Override
    String getObjectType() {
        return MultiMapService.SERVICE_NAME;
    }
}
