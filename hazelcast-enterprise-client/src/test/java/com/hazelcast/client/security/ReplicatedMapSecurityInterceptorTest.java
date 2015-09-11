package com.hazelcast.client.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
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
        interceptor.setExpectation(getObjectType(), objectName, "put", key, val);
        replicatedMap.put(key, val);
    }

    @Test
    public void test2_put() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "put", key, val, ttl, TimeUnit.MILLISECONDS);
        replicatedMap.put(key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void size() {
        interceptor.setExpectation(getObjectType(), objectName, "size");
        replicatedMap.size();
    }

    @Test
    public void isEmpty() {
        interceptor.setExpectation(getObjectType(), objectName, "isEmpty");
        replicatedMap.isEmpty();
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsKey", key);
        replicatedMap.containsKey(key);
    }

    @Test
    public void containsValue() {
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsValue", val);
        replicatedMap.containsValue(val);
    }

    @Test
    public void get() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "get", key);
        replicatedMap.get(key);
    }

    @Test
    public void remove() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "remove", key);
        replicatedMap.remove(key);
    }

    @Test
    public void putAll() {
        final HashMap map = new HashMap();
        map.put(randomString(), randomString());
        map.put(randomString(), randomString());
        map.put(randomString(), randomString());
        interceptor.setExpectation(getObjectType(), objectName, "putAll", map);
        replicatedMap.putAll(map);
    }

    @Test
    public void clear() {
        interceptor.setExpectation(getObjectType(), objectName, "clear");
        replicatedMap.clear();
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", (EntryListener) null);
        replicatedMap.addEntryListener(entryAdapter);
    }

    @Test
    public void test2_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, key);
        replicatedMap.addEntryListener(entryAdapter, key);
    }

    @Test
    public void test3_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, predicate);
        replicatedMap.addEntryListener(entryAdapter, predicate);
    }

    @Test
    public void test4_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, predicate, key);
        replicatedMap.addEntryListener(entryAdapter, predicate, key);
    }

    @Test
    public void removeEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String id = replicatedMap.addEntryListener(entryAdapter);
        interceptor.setExpectation(getObjectType(), objectName, "removeEntryListener", id);
        replicatedMap.removeEntryListener(id);
    }

    @Test
    public void keySet() {
        interceptor.setExpectation(getObjectType(), objectName, "keySet");
        replicatedMap.keySet();
    }

    @Test
    public void values() {
        interceptor.setExpectation(getObjectType(), objectName, "values");
        replicatedMap.values();
    }

    @Test
    public void entrySet() {
        interceptor.setExpectation(getObjectType(), objectName, "entrySet");
        replicatedMap.entrySet();
    }

    @Override
    String getObjectType() {
        return ReplicatedMapService.SERVICE_NAME;
    }

}
