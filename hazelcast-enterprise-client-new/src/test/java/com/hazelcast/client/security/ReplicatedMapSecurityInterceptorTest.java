package com.hazelcast.client.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    ReplicatedMap replicatedMap;

    @Before
    public void setup() {
        objectName = randomString();
        replicatedMap = client.getReplicatedMap(objectName);
    }

    @Test
    public void test1_put() {
        final String key = randomString();
        final String val = randomString();
        replicatedMap.put(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "put", key, val);
    }

    @Test
    public void test2_put() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        replicatedMap.put(key, val, ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "put", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void size() {
        replicatedMap.size();
        interceptor.assertMethod(getObjectType(), objectName, "size");
    }

    @Test
    public void isEmpty() {
        replicatedMap.isEmpty();
        interceptor.assertMethod(getObjectType(), objectName, "isEmpty");
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        replicatedMap.containsKey(key);
        interceptor.assertMethod(getObjectType(), objectName, "containsKey", key);
    }

    @Test
    public void containsValue() {
        final String val = randomString();
        replicatedMap.containsValue(val);
        interceptor.assertMethod(getObjectType(), objectName, "containsValue", val);
    }

    @Test
    public void get() {
        final String key = randomString();
        replicatedMap.get(key);
        interceptor.assertMethod(getObjectType(), objectName, "get", key);
    }

    @Test
    public void remove() {
        final String key = randomString();
        replicatedMap.remove(key);
        interceptor.assertMethod(getObjectType(), objectName, "remove", key);
    }

    @Test
    public void putAll() {
        final HashMap map = new HashMap();
        map.put(randomString(), randomString());
        map.put(randomString(), randomString());
        map.put(randomString(), randomString());
        replicatedMap.putAll(map);
        interceptor.assertMethod(getObjectType(), objectName, "putAll", map);
    }

    @Test
    public void clear() {
        replicatedMap.clear();
        interceptor.assertMethod(getObjectType(), objectName, "clear");
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        replicatedMap.addEntryListener(entryAdapter);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", (EntryListener) null);
    }

    @Test
    public void test2_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        replicatedMap.addEntryListener(entryAdapter, key);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, key);
    }

    @Test
    public void test3_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        replicatedMap.addEntryListener(entryAdapter, predicate);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, predicate);
    }

    @Test
    public void test4_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        final String key = randomString();
        replicatedMap.addEntryListener(entryAdapter, predicate, key);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, predicate, key);
    }

    @Test
    public void removeEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String id = replicatedMap.addEntryListener(entryAdapter);
        interceptor.reset();
        replicatedMap.removeEntryListener(id);
        interceptor.assertMethod(getObjectType(), objectName, "removeEntryListener", id);
    }

    @Test
    public void keySet() {
        replicatedMap.keySet();
        interceptor.assertMethod(getObjectType(), objectName, "keySet");
    }

    @Test
    public void values() {
        replicatedMap.values();
        interceptor.assertMethod(getObjectType(), objectName, "values");
    }

    @Test
    public void entrySet() {
        replicatedMap.entrySet();
        interceptor.assertMethod(getObjectType(), objectName, "entrySet");
    }

    @Override
    String getObjectType() {
        return ReplicatedMapService.SERVICE_NAME;
    }

}
