package com.hazelcast.internal.bplustree;

import com.hazelcast.config.IndexType;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.IndexType.HASH;
import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.internal.bplustree.HDBPlusTree.DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class BPlusTreeStressTest extends BPlusTreeTestSupport {

    private static final int runningTimeSeconds = (int) MINUTES.toSeconds(5);

    @Parameterized.Parameter
    public int indexScanBatchSize;

    @Parameterized.Parameter(1)
    public IndexType indexType;

    @Parameterized.Parameters(name = "indexScanBatchSize: {0} indexType {1}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {0, SORTED},
                {DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE, SORTED},
                {0, HASH},
                {DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE, HASH},
        });
        // @formatter:on
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @Override
    HDBPlusTree newBPlusTree(EnterpriseSerializationService ess,
                             MemoryAllocator keyAllocator,
                             MemoryAllocator indexAllocator, LockManager lockManager,
                             BPlusTreeKeyComparator keyComparator,
                             BPlusTreeKeyAccessor keyAccessor,
                             MapEntryFactory entryFactory,
                             int nodeSize,
                             int indexScanBatchSize0,
                             EntrySlotPayload entrySlotPayload) {
        return HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator, lockManager, keyComparator, keyAccessor,
                entryFactory, nodeSize, indexScanBatchSize, entrySlotPayload);
    }

    @Override
    BPlusTreeKeyComparator newBPlusTreeKeyComparator() {
        return indexType == HASH ? new HashIndexBPlusTreeKeyComparator(ess)
                : new DefaultBPlusTreeKeyComparator(ess);
    }

    @Override
    EntrySlotPayload newEntrySlotPayload() {
        return indexType == HASH ? new HashIndexEntrySlotPayload()
                : new EntrySlotNoPayload();
    }

    @Test(timeout = 600000)
    public void testFullScanAndConcurrentUpdates() {
        int keysCount = 10000;
        insertKeys(keysCount);

        int threadsCount = 2 * Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        AtomicReference<Throwable> exception = new AtomicReference();
        CountDownLatch latch = new CountDownLatch(threadsCount);
        AtomicBoolean stop = new AtomicBoolean();

        for (int i = 0; i < threadsCount; ++i) {

            executor.submit(() -> {
                try {
                    while (!stop.get()) {
                        if (stop.get()) {
                            break;
                        }
                        boolean update = nextBoolean();
                        if (update) {
                            boolean insert = nextBoolean();
                            int index = nextInt(keysCount);
                            if (insert) {
                                insertKey(index);
                            } else {
                                btree.remove(index, nativeData("Name_" + index));
                                insertKey(index);
                            }
                        } else {
                            int queryKeysCount = queryKeysCount();
                            assertTrue("currentCount: " + queryKeysCount, queryKeysCount >= keysCount - threadsCount);
                            assertTrue("currentCount: " + queryKeysCount, queryKeysCount <= keysCount);
                        }
                    }
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                    stop.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }

        sleepAndStop(stop, runningTimeSeconds);
        assertOpenEventually(latch);
        assertNull(exception.get());
        assertEquals(keysCount, queryKeysCount());
    }
}
