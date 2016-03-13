package com.hazelcast.cache;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.spi.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.PooledNativeMemoryStats;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertEnabledFilterRule;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.function.LongLongConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.memory.MemorySize.toPrettyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class CacheNativeMemoryLeakStressTest extends HazelcastTestSupport {

    @Rule
    public final TestRule assertEnabledRule = new AssertEnabledFilterRule();

    private static final int KEY_RANGE = 10000000;
    private static final int MAX_VALUE_SIZE = 1 << 12; // Up to 4K
    private static final int OPERATION_COUNT = 15;
    private static final long TIMEOUT = TimeUnit.SECONDS.toMillis(60);
    private static final MemoryAllocatorType ALLOCATOR_TYPE = MemoryAllocatorType.STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    @Test
    @Category(QuickTest.class)
    public void test_shutdown() throws InterruptedException {
        final Config config = new Config();
        NativeMemoryConfig memoryConfig = config.getNativeMemoryConfig();
        memoryConfig.setEnabled(true)
                .setAllocatorType(MemoryAllocatorType.POOLED)
                .setSize(MEMORY_SIZE);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);

        CacheManager cacheManager = HazelcastServerCachingProvider.createCachingProvider(hz).getCacheManager();
        final String cacheName = randomName();
        CacheConfiguration cacheConfig = new CacheConfig()
                        .setInMemoryFormat(InMemoryFormat.NATIVE)
                        .setEvictionConfig(getEvictionConfig());

        final ICache cache = (ICache) cacheManager.createCache(cacheName, cacheConfig);

        for (int i = 0; i < 1000; i++) {
            cache.put(i, i);
        }

        MemoryStats memoryStats = getNode(hz).hazelcastInstance.getMemoryStats();
        hz.shutdown();

        assertEquals(0, memoryStats.getUsedNative());
        assertEquals(0, memoryStats.getCommittedNative());
        if (memoryStats instanceof PooledNativeMemoryStats) {
            assertEquals(0, ((PooledNativeMemoryStats) memoryStats).getUsedMetadata());
        }
    }

    @Test
    @RequireAssertEnabled
    public void testNativeMemoryLeakWithoutExpiryPolicy() throws InterruptedException {
        testNativeMemoryLeakInternal(null);
    }

    @Test
    @RequireAssertEnabled
    public void testNativeMemoryLeakWithExpiryPolicy() throws InterruptedException {
        testNativeMemoryLeakInternal(new CacheExpiryPolicyFactory());
    }

    private void testNativeMemoryLeakInternal(CacheExpiryPolicyFactory expiryPolicyFactory) throws InterruptedException {
        final Config config = new Config();
        // Set Max Parallel Replications to max value, so that the initial partitions can sync as soon possible.
        // Due to a race condition in object destruction, it can happen that the sync operation takes place
        // while a cache is being destroyed which can result in a memory leak.
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS, String.valueOf(Integer.MAX_VALUE));
        NativeMemoryConfig memoryConfig = config.getNativeMemoryConfig();
        memoryConfig
                .setEnabled(true)
                .setAllocatorType(ALLOCATOR_TYPE)
                .setSize(MEMORY_SIZE);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        warmUpPartitions(hz, hz2);

        CacheManager cacheManager = HazelcastServerCachingProvider.createCachingProvider(hz).getCacheManager();
        final String cacheName = randomName();
        final CacheConfiguration cacheConfig = createCacheConfiguration(expiryPolicyFactory);
        final ICache cache = (ICache) cacheManager.createCache(cacheName, cacheConfig);

        // warm-up
        cache.size();

        final AtomicBoolean done = new AtomicBoolean(false);

        final Thread bouncingThread = new Thread() {
            public void run() {
                while (!done.get()) {
                    HazelcastInstance hz = factory.newHazelcastInstance(config);
                    sleepSeconds(10);
                    terminateInstance(hz);
                    sleepSeconds(5);
                }
            }
        };
        bouncingThread.start();

        final int threads = 8;
        final CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            new WorkerThread(cache, latch).start();
        }

        assertOpenEventually(latch, TIMEOUT * 2);
        done.set(true);
        bouncingThread.join();

        // Even though we wait after node is terminated in `bouncingThread`,
        // be sure that there is no migration on going.
        waitAllForSafeState(hz, hz2);

        AssertionError assertionErrorOnVerifyUsedMemorySizes = null;
        try {
            verifyUsedMemorySizes(cacheName, hz, hz2);
        } catch (AssertionError e) {
            assertionErrorOnVerifyUsedMemorySizes = e;
        }

        cache.destroy();

        try {
            assertTrueEventually(new AssertFreeMemoryTask(hz, hz2), 30);
            if (assertionErrorOnVerifyUsedMemorySizes != null) {
                throw assertionErrorOnVerifyUsedMemorySizes;
            }
        } catch (AssertionError assertionErrorOnVerifyFreeMemorySizes) {
            dumpNativeMemory(hz);
            dumpNativeMemory(hz2);
            if (assertionErrorOnVerifyUsedMemorySizes != null) {
                assertionErrorOnVerifyUsedMemorySizes.printStackTrace();
            }
            throw assertionErrorOnVerifyFreeMemorySizes;
        }
    }

    private void verifyUsedMemorySizes(String cacheName, HazelcastInstance ... hzInstances) {
        for (HazelcastInstance hzInstance : hzInstances) {
            verifyUsedMemorySize(cacheName, hzInstance);
        }
    }

    private void verifyUsedMemorySize(String cacheName, HazelcastInstance hzInstance) {
        assertTrueEventually(new CacheMemorySizeAssertTask(hzInstance, "/hz/" + cacheName));
    }

    private static CacheConfiguration createCacheConfiguration(Factory<ExpiryPolicy> expiryPolicyFactory) {
        CacheConfiguration cacheConfig =
                new CacheConfig()
                        .setBackupCount(1)
                        .setInMemoryFormat(InMemoryFormat.NATIVE)
                        .setEvictionConfig(getEvictionConfig())
                        .setCacheLoaderFactory(new CacheLoaderFactory())
                        .setWriteThrough(true)
                        .setCacheWriterFactory(new CacheWriterFactory());
        if (expiryPolicyFactory != null) {
            cacheConfig.setExpiryPolicyFactory(expiryPolicyFactory);
        }
        return cacheConfig;
    }

    private static EvictionConfig getEvictionConfig() {
        return new EvictionConfig()
                .setSize(95)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    private static class WorkerThread extends Thread {

        private final ICache<Integer, byte[]> cache;
        private final CountDownLatch latch;
        private final Random rand = new Random();

        public WorkerThread(ICache cache, CountDownLatch latch) {
            this.cache = cache;
            this.latch = latch;
        }

        public void run() {
            int counter = 0;
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    int key = rand.nextInt(KEY_RANGE);
                    int op = rand.nextInt(OPERATION_COUNT);
                    doOp(op, key);
                } catch (NativeOutOfMemoryError e) {
                    EmptyStatement.ignore(e);
                } catch (InterruptedException e) {
                    EmptyStatement.ignore(e);
                } catch (CacheWriterException e) {
                    EmptyStatement.ignore(e);
                } catch (EntryProcessorException e) {
                    EmptyStatement.ignore(e);
                } catch (Throwable t) {
                    t.printStackTrace();
                    fail("Should not get this exception!");
                }

                if (++counter % 1000 == 0 && (start + TIMEOUT) < System.currentTimeMillis()) {
                    break;
                }
            }
            latch.countDown();
        }

        private void doOp(int op, int key) throws InterruptedException {
            switch (op) {
                case 0:
                    cache.put(key, newValue(key));
                    break;

                case 1:
                    cache.remove(key);
                    break;

                case 2:
                    cache.replace(key, newValue(key));
                    break;

                case 3:
                    cache.putIfAbsent(key, newValue(key));
                    break;

                case 4:
                    byte[] value = cache.getAndPut(key, newValue(key));
                    verifyValue(key, value);
                    break;

                case 5:
                    value = cache.getAndRemove(key);
                    verifyValue(key, value);
                    break;

                case 6:
                    value = cache.getAndReplace(key, newValue(key));
                    verifyValue(key, value);
                    break;

                case 7:
                    byte[] current = cache.get(key);
                    verifyValue(key, current);
                    if (current != null) {
                        cache.replace(key, current, newValue(key));
                    } else {
                        cache.replace(key, newValue(key));
                    }
                    break;

                case 8:
                    current = cache.get(key);
                    verifyValue(key, current);
                    if (current != null) {
                        cache.remove(key, current);
                    } else {
                        cache.remove(key);
                    }
                    break;

                case 9:
                    Set<Integer> keysToAdd = new HashSet<Integer>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToAdd.add(k);
                    }
                    Map<Integer, byte[]> results = cache.getAll(keysToAdd, null);
                    for (Map.Entry<Integer, byte[]> result : results.entrySet()) {
                        Integer k = result.getKey();
                        byte[] v = result.getValue();
                        verifyValue(k, v);
                    }
                    break;

                case 10:
                    Map<Integer, byte[]> entries = new HashMap<Integer, byte[]>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        entries.put(k, newValue(k));
                    }
                    cache.putAll(entries, null);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        byte[] v = cache.get(k);
                        verifyValue(k, v);
                    }
                    break;

                case 11:
                    Set<Integer> keysToRemove = new HashSet<Integer>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToRemove.add(k);
                    }
                    cache.removeAll(keysToRemove);
                    break;

                case 12:
                    Set<Integer> keysToLoad = new HashSet<Integer>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToLoad.add(k);
                    }
                    CacheLoaderCompletionListener completionListener = new CacheLoaderCompletionListener();
                    cache.loadAll(keysToLoad, true, completionListener);
                    assertTrue(completionListener.done.await(1, TimeUnit.MINUTES));
                    assertNull("Got error on cache::load: " + completionListener.error, completionListener.error.get());
                    Map<Integer, byte[]> loadResults = cache.getAll(keysToLoad, null);
                    for (Map.Entry<Integer, byte[]> result : loadResults.entrySet()) {
                        Integer k = result.getKey();
                        byte[] v = result.getValue();
                        verifyValue(k, v);
                    }
                    break;

                case 13:
                    value = cache.invoke(key, new CacheEntryProcessor());
                    verifyValue(key, value);
                    break;

                case 14:
                    Set<Integer> keysToInvoke = new HashSet<Integer>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToInvoke.add(k);
                    }
                    Map<Integer, EntryProcessorResult<byte[]>> resultEntries =
                            cache.invokeAll(keysToInvoke, new CacheEntryProcessor());
                    for (Map.Entry<Integer, EntryProcessorResult<byte[]>> resultEntry : resultEntries.entrySet()) {
                        Integer k = resultEntry.getKey();
                        EntryProcessorResult<byte[]> r = resultEntry.getValue();
                        verifyValue(k, r.get());
                    }
                    break;

                default:
                    cache.put(key, newValue(key));
            }
        }

        private byte[] newValue(int k) {
            return CacheNativeMemoryLeakStressTest.newValue(rand, k);
        }

    }

    private static void verifyValue(int key, byte[] value) {
        if (value != null) {
            assertEquals(key, Bits.readIntB(value, 0));
            assertEquals(key, Bits.readIntB(value, value.length - 4));
        }
    }

    private static byte[] newValue(Random rand, int k) {
        int len = 16 + rand.nextInt(MAX_VALUE_SIZE); // up to 4k
        byte[] value = new byte[len];
        rand.nextBytes(value);

        Bits.writeIntB(value, 0, k);
        Bits.writeIntB(value, len - 4, k);

        return value;
    }

    private static class CacheExpiryPolicyFactory implements Factory<ExpiryPolicy> {

        @Override
        public ExpiryPolicy create() {
            return new HazelcastExpiryPolicy(
                    new Duration(TimeUnit.MILLISECONDS, 1),
                    new Duration(TimeUnit.MILLISECONDS, 1),
                    new Duration(TimeUnit.MILLISECONDS, 1));
        }

    }

    private static class CacheLoaderFactory implements Factory<CacheLoader<Integer, byte[]>> {

        private static final Random rand = new Random();

        @Override
        public CacheLoader<Integer, byte[]> create() {
            return new CacheLoader<Integer, byte[]>() {
                @Override
                public byte[] load(Integer key) throws CacheLoaderException {
                    return newValue(rand, key);
                }

                @Override
                public Map<Integer, byte[]> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
                    Map<Integer, byte[]> entries = new HashMap<Integer, byte[]>();
                    for (Integer key : keys) {
                        entries.put(key, load(key));
                    }
                    return entries;
                }
            };
        }

    }

    private static class CacheLoaderCompletionListener implements CompletionListener {

        final CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<Exception>();

        @Override
        public void onCompletion() {
            done.countDown();
        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
            error.set(e);
            done.countDown();
        }

    }

    private static class CacheWriterFactory implements Factory<CacheWriter<Integer, byte[]>> {

        @Override
        public CacheWriter<Integer, byte[]> create() {
            return new CacheWriter<Integer, byte[]>() {
                @Override
                public void write(Cache.Entry<? extends Integer, ? extends byte[]> entry)
                        throws CacheWriterException {
                    Integer keyValue = entry.getKey().intValue();
                    if (keyValue % 1000 == 0) {
                        throw new CacheWriterException("Key value is invalid: " + keyValue);
                    }
                }

                @Override
                public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends byte[]>> entries)
                        throws CacheWriterException {
                    for (Cache.Entry<? extends Integer, ? extends byte[]> entry : entries) {
                        write(entry);
                    }
                }

                @Override
                public void delete(Object key) throws CacheWriterException {
                    Integer keyValue = (Integer) key;
                    if (keyValue % 1000 == 0) {
                        throw new CacheWriterException("Key value is invalid: " + keyValue);
                    }
                }

                @Override
                public void deleteAll(Collection<?> keys) throws CacheWriterException {
                    for (Object key : keys) {
                        delete(key);
                    }
                }
            };
        }

    }

    private static class CacheEntryProcessor
            implements EntryProcessor<Integer, byte[], byte[]>, Serializable {

        private static final Random rand = new Random();

        @Override
        public byte[] process(MutableEntry<Integer, byte[]> entry, Object... arguments) throws EntryProcessorException {
            byte[] value = entry.getValue();
            if (rand.nextInt(2) == 0) {
                entry.setValue(newValue(rand, entry.getKey()));
            } else {
                entry.remove();
            }
            return value;
        }

    }

    private static class CacheMemorySizeAssertTask extends AssertTask {

        private final HazelcastInstance instance;
        private final String cacheNameWithPrefix;
        private final int partitionCount;
        private final InternalOperationService operationService;
        private final EnterpriseCacheService cacheService;

        private CacheMemorySizeAssertTask(HazelcastInstance instance, String cacheNameWithPrefix) {
            this.instance = instance;
            this.cacheNameWithPrefix = cacheNameWithPrefix;

            NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
            this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
            this.operationService = nodeEngine.getOperationService();
            this.cacheService = nodeEngine.getService(EnterpriseCacheService.SERVICE_NAME);
        }

        @Override
        public void run() throws Exception {
            final AtomicLong actualMemorySize = new AtomicLong();
            final CountDownLatch latch = new CountDownLatch(partitionCount);

            for (int i = 0; i < partitionCount; i++) {
                PartitionedCacheMemorySizeTask cacheMemorySizeTask =
                    new PartitionedCacheMemorySizeTask(cacheService, cacheNameWithPrefix, i, actualMemorySize, latch);
                operationService.execute(cacheMemorySizeTask);
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));

            final long expectedMemorySize = cacheService.getOrCreateHiDensityCacheInfo(cacheNameWithPrefix).getUsedMemory();
            assertEquals("Expected and actual memory usage sizes are not equal on " + instance,
                         expectedMemorySize, actualMemorySize.get());
        }

    }

    private static class PartitionedCacheMemorySizeTask implements PartitionSpecificRunnable {

        private final EnterpriseCacheService cacheService;
        private final String cacheNameWithPrefix;
        private final int partitionId;
        private final AtomicLong actualMemorySize;
        private final CountDownLatch latch;

        private PartitionedCacheMemorySizeTask(EnterpriseCacheService cacheService, String cacheNameWithPrefix, int partitionId,
                                               AtomicLong actualMemorySize, CountDownLatch latch) {
            this.cacheService = cacheService;
            this.cacheNameWithPrefix = cacheNameWithPrefix;
            this.partitionId = partitionId;
            this.actualMemorySize = actualMemorySize;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                HiDensityCacheRecordStore cacheRecordStore =
                        (HiDensityCacheRecordStore) cacheService.getRecordStore(cacheNameWithPrefix, partitionId);
                if (cacheRecordStore != null) {
                    HiDensityRecordProcessor cacheRecordProcessor = cacheRecordStore.getRecordProcessor();
                    for (Map.Entry<Data, CacheRecord> entry : cacheRecordStore.getReadOnlyRecords().entrySet()) {
                        NativeMemoryData key = (NativeMemoryData) entry.getKey();
                        HiDensityNativeMemoryCacheRecord record = (HiDensityNativeMemoryCacheRecord) entry.getValue();
                        actualMemorySize.addAndGet(cacheRecordProcessor.getSize(key));
                        actualMemorySize.addAndGet(cacheRecordProcessor.getSize(record));
                        actualMemorySize.addAndGet(cacheRecordProcessor.getSize(record.getValue()));
                    }
                }
            } finally {
                latch.countDown();
            }
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

    }

    private static class AssertFreeMemoryTask extends AssertTask {

        private final MemoryStats memoryStats;
        private final MemoryStats memoryStats2;

        private AssertFreeMemoryTask(HazelcastInstance hz, HazelcastInstance hz2) {
            memoryStats = getNode(hz).hazelcastInstance.getMemoryStats();
            memoryStats2 = getNode(hz2).hazelcastInstance.getMemoryStats();
        }

        @Override
        public void run() throws Exception {
            String message = "Node1: " + toPrettyString(memoryStats.getUsedNative())
                             + ", Node2: " + toPrettyString(memoryStats2.getUsedNative());

            assertEquals(message, 0, memoryStats.getUsedNative());
            assertEquals(message, 0, memoryStats2.getUsedNative());
        }

    }

    private static void dumpNativeMemory(HazelcastInstance hz) {
        Node node = getNode(hz);
        EnterpriseSerializationService ss = (EnterpriseSerializationService) node.getSerializationService();
        HazelcastMemoryManager memoryManager = ss.getMemoryManager();

        if (!(memoryManager instanceof StandardMemoryManager)) {
            System.err.println("Cannot dump memory for " + memoryManager);
            return;
        }

        StandardMemoryManager standardMemoryManager = (StandardMemoryManager) memoryManager;
        standardMemoryManager.forEachAllocatedBlock(new LongLongConsumer() {

            private int k;

            @Override
            public void accept(long key, long value) {
                if (value == HiDensityNativeMemoryCacheRecord.SIZE) {
                    HiDensityNativeMemoryCacheRecord record = new HiDensityNativeMemoryCacheRecord(null, key);
                    System.err.println((++k) + ". Record Address: " + key
                                        + " (Value Address: " + record.getValueAddress() + ")");
                } else if (value == 13) {
                    System.err.println((++k) + ". Key Address: " + key);
                } else {
                    System.err.println((++k) + ". Value Address: " + key + ", size: " + value);
                }
            }

        });
    }

}
