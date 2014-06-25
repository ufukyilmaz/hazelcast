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

package com.hazelcast.security;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapService;
import com.hazelcast.query.Predicate;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class MapSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void containsKey() {
        final String key = randomString();
        getMap().containsKey(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void containsValue() {
        final String key = randomString();
        getMap().containsValue(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void get() {
        final String key = randomString();
        getMap().get(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test1_put() {
        final String key = randomString();
        final String val = randomString();
        getMap().put(key, val);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val);
    }

    @Test
    public void test2_put() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        getMap().put(key, val, ttl, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_remove() {
        final String key = randomString();
        getMap().remove(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test2_remove() {
        final String key = randomString();
        final String val = randomString();
        getMap().remove(key, val);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val);
    }

    @Test
    public void delete() {
        final String key = randomString();
        getMap().delete(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void flush() {
        getMap().flush();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void getAsync() {
        final String key = randomString();
        getMap().getAsync(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test1_putAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final String val = randomString();
        getMap().putAsync(key, val).get();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val);
    }

    @Test
    public void test2_putAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        getMap().putAsync(key, val, ttl, TimeUnit.MILLISECONDS).get();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void removeAsync() throws ExecutionException, InterruptedException {
        final String key = randomString();
        getMap().removeAsync(key).get();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void tryRemove() {
        final String key = randomString();
        long timeout = randomLong();
        getMap().tryRemove(key, timeout, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryPut() {
        final String key = randomString();
        final String val = randomString();
        final long timeout = randomLong();
        getMap().tryPut(key, val, timeout, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void putTransient() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        getMap().putTransient(key, val, ttl, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        getMap().putIfAbsent(key, val);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val);
    }

    @Test
    public void test2_putIfAbsent() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        getMap().putIfAbsent(key, val, ttl, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_replace() {
        final String key = randomString();
        final String val = randomString();
        getMap().replace(key, val);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val);
    }

    @Test
    public void test2_replace() {
        final String key = randomString();
        final String val1 = randomString();
        final String val2 = randomString();
        getMap().replace(key, val1, val2);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val1, val2);
    }

    @Test
    public void test1_set() {
        final String key = randomString();
        final String val = randomString();
        getMap().set(key, val);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val);
    }

    @Test
    public void test2_set() {
        final String key = randomString();
        final String val = randomString();
        final long ttl = randomLong();
        getMap().set(key, val, ttl, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, val, ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test1_lock() {
        final String key = randomString();
        getMap().lock(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test2_lock() {
        final String key = randomString();
        final long lease = randomLong();
        getMap().lock(key, lease, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, lease, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isLocked() {
        final String key = randomString();
        getMap().isLocked(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test1_tryLock() {
        final String key = randomString();
        getMap().tryLock(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final String key = randomString();
        final long timeout = randomLong();
        getMap().tryLock(key, timeout, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void unlock() {
        final String key = randomString();
        final IMap map = getMap();
        map.lock(key);
        interceptor.reset();
        map.unlock(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void forceUnlock() {
        final String key = randomString();
        getMap().forceUnlock(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void addInterceptor() {
        final DummyMapInterceptor mapInterceptor = new DummyMapInterceptor(randomLong());
        getMap().addInterceptor(mapInterceptor);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, mapInterceptor);
    }

    @Test
    public void removeInterceptor() {
        final String key = randomString();
        getMap().removeInterceptor(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void test1_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        getMap().addEntryListener(entryAdapter, false);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, null, false);
    }

    @Test
    public void test2_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        getMap().addEntryListener(entryAdapter, key, false);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, null, key, false);
    }

    @Test
    public void test3_addEntryListener() {
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        getMap().addEntryListener(entryAdapter, predicate, false);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, null, predicate, false);
    }

    @Test
    public void test4_addEntryListener() {
        final String key = randomString();
        final EntryAdapter entryAdapter = new EntryAdapter();
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        getMap().addEntryListener(entryAdapter, predicate, key, false);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, null, predicate, key, false);
    }

    @Test
    public void removeEntryListener() {
        final IMap map = getMap();
        final EntryAdapter entryAdapter = new EntryAdapter();
        final String id = map.addEntryListener(entryAdapter, false);
        interceptor.reset();
        map.removeEntryListener(id);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, id);
    }

    @Test
    public void getEntryView() {
        final String key = randomString();
        getMap().getEntryView(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void evict() {
        final String key = randomString();
        getMap().evict(key);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key);
    }

    @Test
    public void evictAll() {
        getMap().evictAll();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void test1_keySet() {
        getMap().keySet();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void test2_keySet() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        getMap().keySet(predicate);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, predicate);
    }

    @Test
    public void test1_values() {
        getMap().values();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void test2_values() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        getMap().values(predicate);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, predicate);
    }

    @Test
    public void test1_entrySet() {
        getMap().entrySet();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void test2_entrySet() {
        final DummyPredicate predicate = new DummyPredicate(randomLong());
        getMap().entrySet(predicate);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, predicate);
    }

    @Test
    public void getAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        getMap().getAll(keys);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, keys);
    }

    @Test
    public void addIndex() {
        final String key = randomString();
        getMap().addIndex(key, false);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, false);
    }

    @Test
    public void executeOnKey() {
        final String key = randomString();
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        getMap().executeOnKey(key, entryProcessor);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, entryProcessor);
    }

    @Test
    public void submitToKey() throws ExecutionException, InterruptedException {
        final String key = randomString();
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        getMap().submitToKey(key, entryProcessor).get();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, key, entryProcessor);
    }

    @Test
    public void test1_executeOnEntries() {
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        getMap().executeOnEntries(entryProcessor);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, entryProcessor);
    }

    @Test
    public void test2_executeOnEntries() {
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        final DummyPredicate predicate = new DummyPredicate();
        getMap().executeOnEntries(entryProcessor, predicate);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, entryProcessor, predicate);
    }

    @Test
    public void executeOnKeys() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        final DummyEntryProcessor entryProcessor = new DummyEntryProcessor(randomLong());
        getMap().executeOnKeys(keys, entryProcessor);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, keys, entryProcessor);
    }

    @Test
    public void size() {
        getMap().size();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void isEmpty() {
        getMap().isEmpty();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void clear() {
        getMap().clear();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void putAll() {
        final HashMap hashMap = new HashMap();
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        hashMap.put(randomString(), randomString());
        getMap().putAll(hashMap);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, hashMap);
    }

    @Test
    public void test1_loadAll() {
        IMap<Object, Object> map = client.getMap("loadAll" + randomString());
        map.loadAll(true);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, true);
    }

    @Test
    public void test2_loadAll() {
        final HashSet keys = new HashSet();
        keys.add(randomString());
        keys.add(randomString());
        keys.add(randomString());
        IMap<Object, Object> map = client.getMap("loadAll" + randomString());
        map.loadAll(keys, true);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, keys, true);
    }

    @Override
    String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public static IMap getMap() {
        return client.getMap(randomString());
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

    static class DummyPredicate implements Predicate {

        long i;

        DummyPredicate() {
        }

        DummyPredicate(final long i) {
            this.i = i;
        }

        @Override
        public boolean apply(final Map.Entry mapEntry) {
            return false;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final DummyPredicate that = (DummyPredicate) o;

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
