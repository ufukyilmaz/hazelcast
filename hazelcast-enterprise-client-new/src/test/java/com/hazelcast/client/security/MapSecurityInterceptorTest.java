/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class MapSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    IMap map;

    @Before
    public void setup() {
        objectName = randomString();
        map = client.getMap(objectName);
    }

    @Test
    public void containsKey() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsKey", key);
        map.containsKey(key);
    }

    @Test
    public void containsValue() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "containsValue", key);
        map.containsValue(key);
    }

    @Test
    public void get() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "get", key);
        map.get(key);
    }

    @Test
    public void test1_put() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "put", key, val);
        map.put(key, val);
    }

    @Test
    public void test2_put() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "put", key, val, ttl, TimeUnit.MILLISECONDS);
        map.put(key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_remove() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "remove", key);
        map.remove(key);
    }

    @Test
    public void test2_remove() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "remove", key, val);
        map.remove(key, val);
    }

    @Test
    public void delete() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "delete", key);
        map.delete(key);
    }

    @Test
    public void flush() {
        interceptor.setExpectation(getObjectType(), objectName, "flush");
        map.flush();
    }

    @Test
    public void getAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "getAsync", key);
        map.getAsync(key).get();
    }

    @Test
    public void test1_putAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "putAsync", key, val);
        map.putAsync(key, val).get();
    }

    @Test
    public void test2_putAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "putAsync", key, val, ttl, TimeUnit.MILLISECONDS);
        map.putAsync(key, val, ttl, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void removeAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "removeAsync", key);
        map.removeAsync(key).get();
    }

    @Test
    public void tryRemove() {
        final String key = randomString();
        long timeout = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "tryRemove", key, timeout, TimeUnit.MILLISECONDS);
        map.tryRemove(key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryPut() {
        final String key = randomString();
        final String val = randomString();
        final long timeout = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "tryPut", key, val, timeout, TimeUnit.MILLISECONDS);
        map.tryPut(key, val, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void putTransient() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "putTransient", key, val, ttl, TimeUnit.MILLISECONDS);
        map.putTransient(key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "putIfAbsent", key, val);
        map.putIfAbsent(key, val);
    }

    @Test
    public void test2_putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "putIfAbsent", key, val, ttl, TimeUnit.MILLISECONDS);
        map.putIfAbsent(key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_replace() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "replace", key, val);
        map.replace(key, val);
    }

    @Test
    public void test2_replace() {
        final String key = randomString();
        final String val1 = randomString();
        final String val2 = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "replace", key, val1, val2);
        map.replace(key, val1, val2);
    }

    @Test
    public void test1_set() {
        final String key = randomString();
        final String val = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "set", key, val);
        map.set(key, val);
    }

    @Test
    public void test2_set() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "set", key, val, ttl, TimeUnit.MILLISECONDS);
        map.set(key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_lock() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "lock", key);
        map.lock(key);
    }

    @Test
    public void test2_lock() {
        final String key = randomString();
        final long lease = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "lock", key, lease, TimeUnit.MILLISECONDS);
        map.lock(key, lease, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isLocked() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "isLocked", key);
        map.isLocked(key);
    }

    @Test
    public void test1_tryLock() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", key);
        map.tryLock(key);
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final String key = randomString();
        final long timeout = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", key, timeout, TimeUnit.MILLISECONDS);
        map.tryLock(key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void unlock() {
        final String key = randomString();
        map.lock(key);
        interceptor.setExpectation(getObjectType(), objectName, "unlock", key);
        map.unlock(key);
    }

    @Test
    public void forceUnlock() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "forceUnlock", key);
        map.forceUnlock(key);
    }

    @Test
    public void addInterceptor() {
        final DummyMapInterceptor mapInterceptor = new DummyMapInterceptor(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "addInterceptor", mapInterceptor);
        map.addInterceptor(mapInterceptor);
    }

    @Test
    public void removeInterceptor() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "removeInterceptor", key);
        map.removeInterceptor(key);
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, false);
        map.addEntryListener(entryAdapter, false);
    }

    @Test
    public void test2_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, key, false);
        map.addEntryListener(entryAdapter, key, false);
    }

    @Test
    public void test3_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, predicate, false);
        map.addEntryListener(entryAdapter, predicate, false);
    }

    @Test
    public void test4_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "addEntryListener", null, predicate, key, false);
        map.addEntryListener(entryAdapter, predicate, key, false);
    }

    @Test
    public void removeEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String id = map.addEntryListener(entryAdapter, false);
        interceptor.setExpectation(getObjectType(), objectName, "removeEntryListener", id);
        map.removeEntryListener(id);
    }

    @Test
    public void getEntryView() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "getEntryView", key);
        map.getEntryView(key);
    }

    @Test
    public void evict() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "evict", key);
        map.evict(key);
    }

    @Test
    public void evictAll() {
        interceptor.setExpectation(getObjectType(), objectName, "evictAll");
        map.evictAll();
    }

    @Test
    public void test1_keySet() {
        interceptor.setExpectation(getObjectType(), objectName, "keySet");
        map.keySet();
    }

    @Test
    public void test2_keySet() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "keySet", predicate);
        map.keySet(predicate);
    }

    @Test
    public void test1_values() {
        interceptor.setExpectation(getObjectType(), objectName, "values");
        map.values();
    }

    @Test
    public void test2_values() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "values", predicate);
        map.values(predicate);
    }

    @Test
    public void test1_entrySet() {
        interceptor.setExpectation(getObjectType(), objectName, "entrySet");
        map.entrySet();
    }

    @Test
    public void test2_entrySet() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "entrySet", predicate);
        map.entrySet(predicate);
    }

    @Test
    public void getAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "getAll", keys);
        map.getAll(keys);
    }

    @Test
    public void addIndex() {
        final String key = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "addIndex", key, false);
        map.addIndex(key, false);
    }

    @Test
    public void executeOnKey() {
        final String key = randomString();
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "executeOnKey", key, entryProcessor);
        map.executeOnKey(key, entryProcessor);
    }

    @Test
    public void submitToKey() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "submitToKey", key, entryProcessor);
        map.submitToKey(key, entryProcessor).get();
    }

    @Test
    public void test1_executeOnEntries() {
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "executeOnEntries", entryProcessor);
        map.executeOnEntries(entryProcessor);
    }

    @Test
    public void test2_executeOnEntries() {
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        final DummyPredicate predicate = new DummyPredicate();
        interceptor.setExpectation(getObjectType(), objectName, "executeOnEntries", entryProcessor, predicate);
        map.executeOnEntries(entryProcessor, predicate);
    }

    @Test
    public void executeOnKeys() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "executeOnKeys", keys, entryProcessor);
        map.executeOnKeys(keys, entryProcessor);
    }

    @Test
    public void size() {
        interceptor.setExpectation(getObjectType(), objectName, "size");
        map.size();
    }

    @Test
    public void isEmpty() {
        interceptor.setExpectation(getObjectType(), objectName, "isEmpty");
        map.isEmpty();
    }

    @Test
    public void clear() {
        interceptor.setExpectation(getObjectType(), objectName, "clear");
        map.clear();
    }

    @Test
    public void putAll() {
        final HashMap hashMap = new HashMap();
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        interceptor.setExpectation(getObjectType(), objectName, "putAll", hashMap);
        map.putAll(hashMap);
    }

    @Test
    public void test1_loadAll() {
        objectName = "loadAll" + randomString();
        IMap<Object, Object> map = client.getMap(objectName);
        interceptor.setExpectation(getObjectType(), objectName, "loadAll", true);
        map.loadAll(true);
    }

    @Test
    public void test2_loadAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        objectName = "loadAll" + randomString();
        IMap<Object, Object> map = client.getMap(objectName);
        interceptor.setExpectation(getObjectType(), objectName, "loadAll", keys, true);
        map.loadAll(keys, true);
    }

    @Test
    public void addPartitionLostListener() {
        final HashMap hashMap = new HashMap();
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        interceptor.setExpectation(getObjectType(), objectName, "addPartitionLostListener", new Object[]{null});
        map.addPartitionLostListener(mock(MapPartitionLostListener.class));
    }

    @Test
    public void removePartitionLostListener() {
        String id = map.addPartitionLostListener(mock(MapPartitionLostListener.class));
        interceptor.setExpectation(getObjectType(), objectName, "removePartitionLostListener", id);
        map.removePartitionLostListener(id);
    }

    @Override
    String getObjectType() {
        return MapService.SERVICE_NAME;
    }

    static class DummyMapInterceptor implements MapInterceptor {

        long i;

        DummyMapInterceptor() {
        }

        DummyMapInterceptor(final long i) {
            this.i = i;
        }

        @Override
        public Object interceptGet(final Object value) {
            return null;
        }

        @Override
        public void afterGet(final Object value) {

        }

        @Override
        public Object interceptPut(final Object oldValue, final Object newValue) {
            return null;
        }

        @Override
        public void afterPut(final Object value) {

        }

        @Override
        public Object interceptRemove(final Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(final Object value) {

        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DummyMapInterceptor that = (DummyMapInterceptor) o;

            if (i != that.i) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return (int) (i ^ (i >>> 32));
        }
    }

    static class DummyEntryProcessor implements EntryProcessor {

        long i;

        DummyEntryProcessor() {
        }

        DummyEntryProcessor(final long i) {
            this.i = i;
        }

        @Override
        public Object process(final Map.Entry entry) {
            return null;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DummyEntryProcessor that = (DummyEntryProcessor) o;

            if (i != that.i) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return (int) (i ^ (i >>> 32));
        }
    }


}
