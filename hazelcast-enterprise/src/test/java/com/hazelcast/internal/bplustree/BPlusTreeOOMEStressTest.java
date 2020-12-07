package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class BPlusTreeOOMEStressTest extends BPlusTreeTestSupport {

    private static final int runningTimeSeconds = (int) MINUTES.toSeconds(5);
    private StandardMemoryManager externalAllocator = new StandardMemoryManager(new MemorySize(1, MemoryUnit.GIGABYTES));

    @Override
    MemorySize getMemorySize() {
        return new MemorySize(20, MemoryUnit.MEGABYTES);
    }

    @Override
    int getNodeSize() {
        return 128;
    }

    @Override
    float getMetadataPercentage() {
        return 75.0f;
    }

    @Test(timeout = 600000)
    public void testOOMEHandling() {
        int initialKeysCount = 100;
        insertKeys(initialKeysCount);
        int keysToRemove = 10;
        int threadsCount = Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        AtomicReference<Throwable> exception = new AtomicReference();
        CountDownLatch latch = new CountDownLatch(threadsCount);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong keysCount = new AtomicLong(initialKeysCount);
        AtomicLong oomCount = new AtomicLong();


        for (int i = 0; i < threadsCount; ++i) {

            executor.submit(() -> {
                try {
                    while (!stop.get()) {
                        if (stop.get()) {
                            break;
                        }
                        boolean update = nextUpdate();
                        if (update) {
                            int index = nextInt(100000);
                            try {
                                NativeMemoryData oldValue = insertNewKey(index);
                                if (oldValue == null) {
                                    keysCount.incrementAndGet();
                                } else {
                                    ess.disposeData(oldValue, externalAllocator);
                                }
                            } catch (NativeOutOfMemoryError ooe) {
                                oomCount.incrementAndGet();
                                // Remove some entries
                                int startIndex = nextInt(50000);
                                Iterator<Map.Entry<String, String>> it =
                                        btree.lookup(startIndex, true, null, true, false);
                                int count = 0;
                                while (it.hasNext() && count < keysToRemove) {
                                    Map.Entry<String, String> entry = it.next();
                                    String value = entry.getValue();
                                    int indexKey = Integer.parseInt(value.substring(value.indexOf("_") + 1));
                                    NativeMemoryData oldValue = btree.remove(indexKey, null);
                                    if (oldValue != null) {
                                        ess.disposeData(oldValue, externalAllocator);
                                        keysCount.decrementAndGet();
                                    }
                                    ++count;
                                }
                            }
                        } else {
                            // Run full scan which deserializes all data
                            queryKeysCount();
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
        assertEquals(keysCount.get(), queryKeysCount());
        assertTrue(oomCount.get() > 0);
    }

    @Override
    void insertKeys(int count) {
        for (int i = 0; i < count; ++i) {
            insertNewKey(i);
        }
    }

    NativeMemoryData insertNewKey(int index) {
        Integer indexKey = index;
        String mapKey = "Name_" + index;
        String value = "Value_" + index;
        NativeMemoryData mapKeyData = ess.toNativeData(mapKey, externalAllocator);
        NativeMemoryData valueData = ess.toNativeData(value, externalAllocator);
        return btree.insert(indexKey, mapKeyData, valueData);
    }

    boolean nextUpdate() {
        return nextInt(100) < 90;
    }
}
