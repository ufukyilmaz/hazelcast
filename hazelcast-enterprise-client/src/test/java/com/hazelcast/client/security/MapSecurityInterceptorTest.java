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
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
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
        map.containsKey(key);
        interceptor.assertMethod(getObjectType(), objectName, "containsKey", key);
    }

    @Test
    public void containsValue() {
        final String key = randomString();
        map.containsValue(key);
        interceptor.assertMethod(getObjectType(), objectName, "containsValue", key);
    }

    @Test
    public void get() {
        final String key = randomString();
        map.get(key);
        interceptor.assertMethod(getObjectType(), objectName, "get", key);
    }

    @Test
    public void test1_put() {
        final String key = randomString();
        final String val = randomString();
        map.put(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "put", key, val);
    }

    @Test
    public void test2_put() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        map.put(key, val, ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "put", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_remove() {
        final String key = randomString();
        map.remove(key);
        interceptor.assertMethod(getObjectType(), objectName, "remove", key);
    }

    @Test
    public void test2_remove() {
        final String key = randomString();
        final String val = randomString();
        map.remove(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "remove", key, val);
    }

    @Test
    public void delete() {
        final String key = randomString();
        map.delete(key);
        interceptor.assertMethod(getObjectType(), objectName, "delete", key);
    }

    @Test
    public void flush() {
        map.flush();
        interceptor.assertMethod(getObjectType(), objectName, "flush");
    }

    @Test
    public void getAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        map.getAsync(key).get();
        interceptor.assertMethod(getObjectType(), objectName, "getAsync", key);
    }

    @Test
    public void test1_putAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final String val = randomString();
        map.putAsync(key, val).get();
        interceptor.assertMethod(getObjectType(), objectName, "putAsync", key, val);
    }

    @Test
    public void test2_putAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        map.putAsync(key, val, ttl, TimeUnit.MILLISECONDS).get();
        interceptor.assertMethod(getObjectType(), objectName, "putAsync", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void removeAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        map.removeAsync(key).get();

        interceptor.assertMethod(getObjectType(), objectName, "removeAsync", key);
    }

    @Test
    public void tryRemove() {
        final String key = randomString();
        long timeout = randomLong();
        map.tryRemove(key, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryRemove", key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryPut() {
        final String key = randomString();
        final String val = randomString();
        final long timeout = randomLong();
        map.tryPut(key, val, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryPut", key, val, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void putTransient() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        map.putTransient(key, val, ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "putTransient", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        map.putIfAbsent(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "putIfAbsent", key, val);
    }

    @Test
    public void test2_putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        map.putIfAbsent(key, val, ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "putIfAbsent", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_replace() {
        final String key = randomString();
        final String val = randomString();
        map.replace(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "replace", key, val);
    }

    @Test
    public void test2_replace() {
        final String key = randomString();
        final String val1 = randomString();
        final String val2 = randomString();
        map.replace(key, val1, val2);
        interceptor.assertMethod(getObjectType(), objectName, "replace", key, val1, val2);
    }

    @Test
    public void test1_set() {
        final String key = randomString();
        final String val = randomString();
        map.set(key, val);
        interceptor.assertMethod(getObjectType(), objectName, "set", key, val);
    }

    @Test
    public void test2_set() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        map.set(key, val, ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "set", key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_lock() {
        final String key = randomString();
        map.lock(key);
        interceptor.assertMethod(getObjectType(), objectName, "lock", key);
    }

    @Test
    public void test2_lock() {
        final String key = randomString();
        final long lease = randomLong();
        map.lock(key, lease, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "lock", key, lease, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isLocked() {
        final String key = randomString();
        map.isLocked(key);
        interceptor.assertMethod(getObjectType(), objectName, "isLocked", key);
    }

    @Test
    public void test1_tryLock() {
        final String key = randomString();
        map.tryLock(key);
        interceptor.assertMethod(getObjectType(), objectName, "tryLock", key);
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final String key = randomString();
        final long timeout = randomLong();
        map.tryLock(key, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryLock", key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void unlock() {
        final String key = randomString();
        map.lock(key);
        interceptor.reset();
        map.unlock(key);
        interceptor.assertMethod(getObjectType(), objectName, "unlock", key);
    }

    @Test
    public void forceUnlock() {
        final String key = randomString();
        map.forceUnlock(key);
        interceptor.assertMethod(getObjectType(), objectName, "forceUnlock", key);
    }

    @Test
    public void addInterceptor() {
        final DummyMapInterceptor mapInterceptor = new DummyMapInterceptor(randomLong());
        map.addInterceptor(mapInterceptor);
        interceptor.assertMethod(getObjectType(), objectName, "addInterceptor", mapInterceptor);
    }

    @Test
    public void removeInterceptor() {
        final String key = randomString();
        map.removeInterceptor(key);
        interceptor.assertMethod(getObjectType(), objectName, "removeInterceptor", key);
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        map.addEntryListener(entryAdapter, false);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, false);
    }

    @Test
    public void test2_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        map.addEntryListener(entryAdapter, key, false);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, key, false);
    }

    @Test
    public void test3_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        map.addEntryListener(entryAdapter, predicate, false);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, predicate, false);
    }

    @Test
    public void test4_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        map.addEntryListener(entryAdapter, predicate, key, false);
        interceptor.assertMethod(getObjectType(), objectName, "addEntryListener", null, predicate, key, false);
    }

    @Test
    public void removeEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String id = map.addEntryListener(entryAdapter, false);
        interceptor.reset();
        map.removeEntryListener(id);
        interceptor.assertMethod(getObjectType(), objectName, "removeEntryListener", id);
    }

    @Test
    public void getEntryView() {
        final String key = randomString();
        map.getEntryView(key);
        interceptor.assertMethod(getObjectType(), objectName, "getEntryView", key);
    }

    @Test
    public void evict() {
        final String key = randomString();
        map.evict(key);
        interceptor.assertMethod(getObjectType(), objectName, "evict", key);
    }

    @Test
    public void evictAll() {
        map.evictAll();
        interceptor.assertMethod(getObjectType(), objectName, "evictAll");
    }

    @Test
    public void test1_keySet() {
        map.keySet();
        interceptor.assertMethod(getObjectType(), objectName, "keySet");
    }

    @Test
    public void test2_keySet() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        map.keySet(predicate);
        interceptor.assertMethod(getObjectType(), objectName, "keySet", predicate);
    }

    @Test
    public void test1_values() {
        map.values();
        interceptor.assertMethod(getObjectType(), objectName, "values");
    }

    @Test
    public void test2_values() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        map.values(predicate);
        interceptor.assertMethod(getObjectType(), objectName, "values", predicate);
    }

    @Test
    public void test1_entrySet() {
        map.entrySet();
        interceptor.assertMethod(getObjectType(), objectName, "entrySet");
    }

    @Test
    public void test2_entrySet() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        map.entrySet(predicate);
        interceptor.assertMethod(getObjectType(), objectName, "entrySet", predicate);
    }

    @Test
    public void getAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        map.getAll(keys);
        interceptor.assertMethod(getObjectType(), objectName, "getAll", keys);
    }

    @Test
    public void addIndex() {
        final String key = randomString();
        map.addIndex(key, false);
        interceptor.assertMethod(getObjectType(), objectName, "addIndex", key, false);
    }

    @Test
    public void executeOnKey() {
        final String key = randomString();
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        map.executeOnKey(key, entryProcessor);
        interceptor.assertMethod(getObjectType(), objectName, "executeOnKey", key, entryProcessor);
    }

    @Test
    public void submitToKey() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        map.submitToKey(key, entryProcessor).get();
        interceptor.assertMethod(getObjectType(), objectName, "submitToKey", key, entryProcessor);
    }

    @Test
    public void test1_executeOnEntries() {
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        map.executeOnEntries(entryProcessor);
        interceptor.assertMethod(getObjectType(), objectName, "executeOnEntries", entryProcessor);
    }

    @Test
    public void test2_executeOnEntries() {
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        final DummyPredicate predicate = new DummyPredicate();
        map.executeOnEntries(entryProcessor, predicate);
        interceptor.assertMethod(getObjectType(), objectName, "executeOnEntries", entryProcessor, predicate);
    }

    @Test
    public void executeOnKeys() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        map.executeOnKeys(keys, entryProcessor);
        interceptor.assertMethod(getObjectType(), objectName, "executeOnKeys", keys, entryProcessor);
    }

    @Test
    public void size() {
        map.size();
        interceptor.assertMethod(getObjectType(), objectName, "size");
    }

    @Test
    public void isEmpty() {
        map.isEmpty();
        interceptor.assertMethod(getObjectType(), objectName, "isEmpty");
    }

    @Test
    public void clear() {
        map.clear();
        interceptor.assertMethod(getObjectType(), objectName, "clear");
    }

    @Ignore
    @Test
    public void putAll() {
        final HashMap hashMap = new HashMap();
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        map.putAll(hashMap);
        interceptor.assertMethod(getObjectType(), objectName, "putAll", hashMap);
    }

    @Test
    public void test1_loadAll() {
        objectName = "loadAll" + randomString();
        IMap<Object, Object> map = client.getMap(objectName);
        map.loadAll(true);
        interceptor.assertMethod(getObjectType(), objectName, "loadAll", true);
    }

    @Test
    public void test2_loadAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        objectName = "loadAll" + randomString();
        IMap<Object, Object> map = client.getMap(objectName);
        map.loadAll(keys, true);
        interceptor.assertMethod(getObjectType(), objectName, "loadAll", keys, true);
    }

    @Test
    public void addPartitionLostListener() {
        final HashMap hashMap = new HashMap();
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        map.addPartitionLostListener(new MapPartitionLostListener() {
            @Override
            public void partitionLost(MapPartitionLostEvent event) {

            }
        });
        interceptor.assertMethod(getObjectType(), objectName, "addPartitionLostListener", new Object[]{null});
    }

    @Test
    public void removePartitionLostListener() {
        String id = map.addPartitionLostListener(new MapPartitionLostListener() {
            @Override
            public void partitionLost(MapPartitionLostEvent event) {

            }
        });
        map.removePartitionLostListener(id);
        interceptor.assertMethod(getObjectType(), objectName, "removePartitionLostListener", id);
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
