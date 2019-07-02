package com.hazelcast.map;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.merkletree.MerkleTree;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.internal.util.collections.Sets;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMerkleTreeUpdateTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "mapWithMerkleTree";
    private static final int INSTANCE_COUNT = 1;

    private IMap<String, String> map;
    private MerkleTree merkleTree;
    private BasicMapStore basicMapStore;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "method:{0} inMemoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
                {InMemoryFormat.NATIVE},
        });
    }

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        basicMapStore = new BasicMapStore();
        Config config = getConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        map = instance.getMap(MAP_NAME);
    }

    protected Config getConfig() {
        Config cfg = super.getConfig();

        // we run this test with only one partition, since we verify the
        // updates to a partition-specific data structure and this way
        // we don't need to know which key is held on which partition
        cfg.setProperty("hazelcast.partition.count", "1");

        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            cfg = HDTestSupport.getHDConfig(cfg);
        }

        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat)
                 .getMapStoreConfig()
                 .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY)
                 .setEnabled(true)
                 .setImplementation(basicMapStore);
        mapConfig.getMerkleTreeConfig()
           .setEnabled(true)
           .setDepth(4);
        cfg.addMapConfig(mapConfig);
        return cfg;
    }

    @Test
    public void putUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondPutWithSameValueDoesNotUpdateMerkleTree() {
        map.put("42", "42");
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.put("42", "42");
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void putTtlUpdatesMerkleTree() {
        map.put("42", "42", 1, SECONDS);
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondPutTtlWithSameValueDoesNotUpdateMerkleTree() {
        map.put("42", "42", 1, SECONDS);
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.put("42", "42", 1, SECONDS);
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void putAsyncUpdatesMerkleTree() throws Exception {
        ICompletableFuture<String> future = map.putAsync("42", "42");
        future.get();
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondPutAsyncWithSameValueDoesNotUpdateMerkleTree() throws Exception {
        ICompletableFuture<String> future = map.putAsync("42", "42");
        future.get();
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        future = map.putAsync("42", "42");
        future.get();
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void putAsyncTtlUpdatesMerkleTree() throws Exception {
        ICompletableFuture<String> future = map.putAsync("42", "42", 1, SECONDS);
        future.get();
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondPutAsyncTtlWithSameValueDoesNotUpdateMerkleTree() throws Exception {
        ICompletableFuture<String> future = map.putAsync("42", "42", 1, SECONDS);
        future.get();
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        future = map.putAsync("42", "42", 1, SECONDS);
        future.get();
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void putAllUpdatesMerkleTree() {
        Map<String, String> mapForPutAll = new HashMap<String, String>();
        mapForPutAll.put("42", "42");
        map.putAll(mapForPutAll);
        assertNotEquals(0, rootHash());
    }

    @Test
    public void putTransientUpdatesMerkleTree() {
        map.putTransient("42", "42", 1, SECONDS);
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondPutTransientWithSameValueDoesNotUpdateMerkleTree() {
        map.putTransient("42", "42", 1, SECONDS);
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.putTransient("42", "42", 1, SECONDS);
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void putIfAbsentUpdatesMerkleTree() {
        map.putIfAbsent("42", "42");
        assertNotEquals(0, rootHash());
    }

    @Test
    public void putIfAbsentTtlUpdatesMerkleTree() {
        map.putIfAbsent("42", "42", 1, SECONDS);
        assertNotEquals(0, rootHash());
    }

    @Test
    public void tryPutUpdatesMerkleTree() {
        map.tryPut("42", "42", 1, SECONDS);
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondTryPutWithSameValueDoesNotUpdateMerkleTree() {
        map.tryPut("42", "42", 1, SECONDS);
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.tryPut("42", "42", 1, SECONDS);
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void setUpdatesMerkleTree() {
        map.set("42", "42");
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondSetWithSameValueDoesNotUpdateMerkleTree() {
        map.set("42", "42");
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.set("42", "42");
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void setWithTtlUpdatesMerkleTree() {
        map.set("42", "42", 1, SECONDS);
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondSetTtlWithSameValueDoesNotUpdateMerkleTree() {
        map.set("42", "42", 1, SECONDS);
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.set("42", "42", 1, SECONDS);
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void setAsyncUpdatesMerkleTree() throws Exception {
        ICompletableFuture<Void> future = map.setAsync("42", "42", 1, SECONDS);
        future.get();
        assertNotEquals(0, rootHash());
    }

    @Test
    public void secondSetAsyncWithSameValueDoesNotUpdateMerkleTree() throws Exception {
        ICompletableFuture<Void> future = map.setAsync("42", "42", 1, SECONDS);
        future.get();
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        future = map.setAsync("42", "42", 1, SECONDS);
        future.get();
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void setTtlDoesNotUpdateMerkleTree() {
        map.put("42", "42");
        int rootHashBeforeSetTtl = rootHash();
        map.setTtl("42", 1, SECONDS);
        assertEquals(rootHashBeforeSetTtl, rootHash());
    }

    @Test
    public void replaceUpdatesMerkleTree() {
        map.put("42", "42");
        final int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.replace("42", "42x");
        assertNotEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void replaceToSameValueDoesNotUpdateMerkleTree() {
        map.put("42", "42");
        int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.replace("42", "42");
        assertEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void replaceWithTestValueUpdatesMerkleTree() {
        map.put("42", "42");
        final int rootHashBeforeReplace = rootHash();
        assertNotEquals(0, rootHashBeforeReplace);
        map.replace("42", "42", "42x");
        assertNotEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void removeUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.remove("42");
        assertEquals(0, rootHash());
    }

    @Test
    public void removeWithTestValueUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.remove("42", "42");
        assertEquals(0, rootHash());
    }

    @Test
    public void removeAsyncUpdatesMerkleTree() throws Exception {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        ICompletableFuture<String> future = map.removeAsync("42");
        future.get();
        assertEquals(0, rootHash());
    }

    @Test
    public void tryRemoveUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.tryRemove("42", 1, SECONDS);
        assertEquals(0, rootHash());
    }

    @Test
    public void removeAllUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.removeAll(new Predicate<String, String>() {

            @Override
            public boolean apply(Map.Entry<String, String> mapEntry) {
                return mapEntry.getKey().equals("42");
            }
        });
        assertEquals(0, rootHash());
    }

    @Test
    public void deleteUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.delete("42");
        assertEquals(0, rootHash());
    }

    @Test
    public void evictUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.evict("42");
        assertEquals(0, rootHash());
    }

    @Test
    public void evictAllUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.evictAll();
        assertEquals(0, rootHash());
    }

    @Test
    public void clearUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.clear();
        assertEquals(0, rootHash());
    }

    @Test
    public void executeOnKeyReplaceUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        final int rootHashBeforeReplace = rootHash();
        map.executeOnKey("42",
                entry -> {
                    entry.setValue("42x");
                    return null;
                });
        assertNotEquals(0, rootHash());
        assertNotEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void executeOnKeyRemoveUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.executeOnKey("42",
                entry -> {
                    entry.setValue(null);
                    return null;
                });
        assertEquals(0, rootHash());
    }

    @Test
    public void executeOnKeysReplaceUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        final int rootHashBeforeReplace = rootHash();
        map.executeOnKeys(Sets.newSet("42"),
                entry -> {
                    entry.setValue("42x");
                    return null;
                });
        assertNotEquals(0, rootHash());
        assertNotEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void executeOnKeysRemoveUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.executeOnKeys(Sets.newSet("42"),
                entry -> {
                    entry.setValue(null);
                    return null;
                });
        assertEquals(0, rootHash());
    }

    @Test
    public void submitToKeyReplaceUpdatesMerkleTree() throws Exception {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        final int rootHashBeforeReplace = rootHash();
        ICompletableFuture future = map.submitToKey("42",
                entry -> {
                    entry.setValue("42x");
                    return null;
                });
        future.get();
        assertNotEquals(0, rootHash());
        assertNotEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void submitToKeyWithCallbackReplaceUpdatesMerkleTree() throws Throwable {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        final int rootHashBeforeReplace = rootHash();
        AwaitableExecutionCallback callback = new AwaitableExecutionCallback();
        map.submitToKey("42",
                entry -> {
                    entry.setValue("42x");
                    return null;
                }, callback);

        callback.await();
        assertNotEquals(0, rootHash());
        assertNotEquals(rootHashBeforeReplace, rootHash());
    }

    @Test
    public void submitToKeyRemoveUpdatesMerkleTree() throws Exception {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        ICompletableFuture future = map.submitToKey("42",
                entry -> {
                    entry.setValue(null);
                    return null;
                });
        future.get();
        assertEquals(0, rootHash());
    }

    @Test
    public void submitToKeyWithCallbackRemoveUpdatesMerkleTree() throws Throwable {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        AwaitableExecutionCallback callback = new AwaitableExecutionCallback();
        map.submitToKey("42",
                entry -> {
                    entry.setValue(null);
                    return null;
                }, callback);

        callback.await();
        assertEquals(0, rootHash());
    }

    @Test
    public void executeOnEntriesReplaceUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        int rootHashBeforeReplace = rootHash();
        map.executeOnEntries(
                entry -> {
                    entry.setValue("42x");
                    return null;
                });

        assertNotEquals(rootHashBeforeReplace, rootHash());
        assertNotEquals(0, rootHash());
    }

    @Test
    public void executeOnEntriesRemoveUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.executeOnEntries(
                entry -> {
                    entry.setValue(null);
                    return null;
                });

        assertEquals(0, rootHash());
    }

    @Test
    public void executeOnEntriesWithPredicateReplaceUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        int rootHashBeforeReplace = rootHash();
        map.executeOnEntries(
                entry -> {
                    entry.setValue("42x");
                    return null;
                }, (Predicate) mapEntry -> mapEntry.getKey().equals("42"));

        assertNotEquals(rootHashBeforeReplace, rootHash());
        assertNotEquals(0, rootHash());
    }

    @Test
    public void executeOnEntriesWithPredicateRemoveUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.executeOnEntries(
                entry -> {
                    entry.setValue(null);
                    return null;
                }, (Predicate) mapEntry -> mapEntry.getKey().equals("42"));

        assertEquals(0, rootHash());
    }

    @Test
    public void loadAllOnEmptyMapUpdatesMerkleTree() {
        basicMapStore.addRecord("42", "42x");
        boolean irrelevant = true;
        map.loadAll(irrelevant);

        waitForRecordLoaded();

        assertNotEquals(0, rootHash());
    }

    @Test
    public void loadAllWithReplaceFalseOnNotEmptyMapDoesNotUpdateMerkleTree() {
        basicMapStore.addRecord("42", "42x");
        map.put("42", "42");
        int rootHashBeforeLoad = rootHash();
        map.loadAll(false);

        assertNotEquals(0, rootHash());
        assertEquals(rootHashBeforeLoad, rootHash());
    }

    @Test
    public void loadAllWithReplaceTrueOnNotEmptyMapUpdatesMerkleTree() {
        map.put("42", "42");
        int rootHashBeforeLoad = rootHash();

        basicMapStore.addRecord("42", "42x");
        map.loadAll(true);

        waitForRecordLoaded();

        assertNotEquals(0, rootHash());
        assertNotEquals(rootHashBeforeLoad, rootHash());
    }

    @Test
    public void loadAllWithKeysWithReplaceFalseOnNotEmptyMapDoesNotUpdateMerkleTree() {
        basicMapStore.addRecord("42", "42x");
        map.put("42", "42");
        int rootHashBeforeLoad = rootHash();
        map.loadAll(Sets.newSet("42"), false);

        assertNotEquals(0, rootHash());
        assertEquals(rootHashBeforeLoad, rootHash());
    }

    @Test
    public void loadAllWithKeysWithReplaceTrueOnNotEmptyMapUpdatesMerkleTree() {
        map.put("42", "42");
        int rootHashBeforeLoad = rootHash();

        basicMapStore.addRecord("42", "42x");
        map.loadAll(Sets.newSet("42"), true);

        waitForRecordLoaded();

        assertNotEquals(0, rootHash());
        assertNotEquals(rootHashBeforeLoad, rootHash());
    }

    private void waitForRecordLoaded() {
        assertTrueEventually(() -> assertEquals("42x", map.get("42")));
    }

    @Test
    public void loadEvictedUpdatesMerkleTree() {
        map.put("42", "42");
        int rootHashBeforeEviction = rootHash();
        map.evict("42");
        assertEquals(0, rootHash());

        // triggering load here
        map.get("42");

        assertEquals(rootHashBeforeEviction, rootHash());
    }

    @Test
    public void destroyUpdatesMerkleTree() {
        map.put("42", "42");
        assertNotEquals(0, rootHash());
        map.destroy();
        assertEquals(0, rootHash());
    }


    private int rootHash() {
        return getMerkleTree(map).getNodeHash(0);
    }

    private MapServiceContext getMapServiceContext(MapProxyImpl map) {
        MapService mapService = (MapService) map.getService();
        return mapService.getMapServiceContext();
    }

    private MerkleTree getMerkleTree(IMap<String, String> map) {
        if (merkleTree == null) {
            MapServiceContext mapServiceContext = getMapServiceContext((MapProxyImpl) map);
            EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) mapServiceContext
                    .getPartitionContainer(0);
            merkleTree = partitionContainer.getMerkleTreeOrNull(map.getName());
        }

        return merkleTree;
    }

    private static final class AwaitableExecutionCallback implements ExecutionCallback {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Throwable throwable;

        @Override
        public void onResponse(Object response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            throwable = t;
            latch.countDown();
        }

        private void await() throws Throwable {
            latch.await();
            if (throwable != null) {
                throw throwable;
            }
        }
    }

    private static class BasicMapStore implements MapStore<String, String> {

        private ConcurrentMap<String, String> records = new ConcurrentHashMap<>();

        private BasicMapStore() {
        }

        private void addRecord(String key, String value) {
            records.put(key, value);
        }

        @Override
        public String load(String key) {
            return records.get(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> loadedRecords = new HashMap<>();
            for (Map.Entry<String, String> entry : records.entrySet()) {
                if (keys.contains(entry.getKey())) {
                    loadedRecords.put(entry.getKey(), entry.getValue());
                }
            }
            return loadedRecords;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            HashSet<String> keys = new HashSet<>();
            keys.addAll(records.keySet());
            return keys;
        }

        @Override
        public void store(String key, String value) {
            records.put(key, value);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            records.putAll(map);
        }

        @Override
        public void delete(String key) {
            records.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            for (String key : keys) {
                records.remove(key);
            }
        }
    }
}
