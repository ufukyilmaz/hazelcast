package com.hazelcast.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class ReplicatedMapSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void test1_put() {
        final String key = randomString();
        final String val = randomString();
        getReplicatedMap().put(key, val);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "put", key, val);
    }

    @Test
    public void test2_put() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        getReplicatedMap().put(key, val, ttl, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "put", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void size() {
        getReplicatedMap().size();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "size");
    }

    @Test
    public void isEmpty() {
        getReplicatedMap().isEmpty();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "isEmpty");
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        getReplicatedMap().containsKey(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "containsKey", key);
    }

    @Test
    public void containsValue() {
        final String val = randomString();
        getReplicatedMap().containsValue(val);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "containsValue", val);
    }

    @Test
    public void get() {
        final String key = randomString();
        getReplicatedMap().get(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "get", key);
    }

    @Test
    public void remove() {
        final String key = randomString();
        getReplicatedMap().remove(key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "remove", key);
    }

    @Test
    public void putAll() {
        final HashMap map = new HashMap();
        map.put(randomString(), randomString());
        map.put(randomString(), randomString());
        map.put(randomString(), randomString());
        getReplicatedMap().putAll(map);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "putAll", map);
    }

    @Test
    public void clear() {
        getReplicatedMap().clear();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "clear");
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        getReplicatedMap().addEntryListener(entryAdapter);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addEntryListener", (EntryListener) null);
    }

    @Test
    public void test2_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        getReplicatedMap().addEntryListener(entryAdapter, key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addEntryListener", null, key);
    }

    @Test
    public void test3_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        getReplicatedMap().addEntryListener(entryAdapter, predicate);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addEntryListener", null, predicate);
    }

    @Test
    public void test4_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        final String key = randomString();
        getReplicatedMap().addEntryListener(entryAdapter, predicate, key);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addEntryListener", null, predicate, key);
    }

    @Test
    public void removeEntryListener() {
        final ReplicatedMap replicatedMap = getReplicatedMap();
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String id = replicatedMap.addEntryListener(entryAdapter);
        interceptor.reset();
        replicatedMap.removeEntryListener(id);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "removeEntryListener", id);
    }

    @Test
    public void keySet() {
        getReplicatedMap().keySet();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "keySet");
    }

    @Test
    public void values() {
        getReplicatedMap().values();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "values");
    }

    @Test
    public void entrySet() {
        getReplicatedMap().entrySet();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "entrySet");
    }

    @Override
    String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    public static ReplicatedMap getReplicatedMap() {
        return client.getReplicatedMap(randomString());
    }
}
