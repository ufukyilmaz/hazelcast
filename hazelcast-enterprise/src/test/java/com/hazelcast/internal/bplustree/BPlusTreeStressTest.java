package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class BPlusTreeStressTest extends BPlusTreeTestSupport {

    private static final int runningTimeSeconds = (int) MINUTES.toSeconds(5);

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
