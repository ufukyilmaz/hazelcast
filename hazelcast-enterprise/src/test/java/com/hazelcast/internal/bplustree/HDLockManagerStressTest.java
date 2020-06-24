package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.bplustree.HDLockManagerTest.assertLockState;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepAndStop;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class HDLockManagerStressTest {

    private static final int runningTimeSeconds = (int) MINUTES.toSeconds(5);

    private HDLockManager lockManager;
    private long lockAddress;
    private HazelcastMemoryManager defaultAllocator;

    @Before
    public void setUp() {
        defaultAllocator = new StandardMemoryManager(new MemorySize(1, MemoryUnit.KILOBYTES));
        int stripesCount = 32;
        lockManager = new HDLockManager(stripesCount);
        lockAddress = allocateZeroed(8);
    }

    @After
    public void tearDown() {
        if (defaultAllocator != null) {
            defaultAllocator.dispose();
        }
    }

    @Test(timeout = 600000)
    public void testWriteLockCorrectness() {
        int threadsCount = 4 * Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        AtomicBoolean stop = new AtomicBoolean();

        MutableLong value = new MutableLong();
        CountDownLatch latch = new CountDownLatch(threadsCount);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicLong expectedCounter = new AtomicLong();

        for (int i = 0; i < threadsCount; ++i) {
            executor.submit(() -> {
                try {
                    int counter = 0;
                    while (!stop.get()) {
                        lockManager.writeLock(lockAddress);
                        value.value++;
                        lockManager.releaseLock(lockAddress);
                        counter++;
                    }
                    expectedCounter.getAndAdd(counter);
                    latch.countDown();
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
        }

        sleepAndStop(stop, runningTimeSeconds);
        assertOpenEventually(latch);
        assertNull(exception.get());
        lockManager.readLock(lockAddress);
        assertEquals(expectedCounter.get(), value.value);
        assertTrue(expectedCounter.get() > 0);
        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        executor.shutdownNow();
    }

    @Test(timeout = 600000)
    public void testReadWriteLockCorrectness() {
        int threadsCount = 4 * Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        AtomicBoolean stop = new AtomicBoolean();

        MutableLong valueX = new MutableLong();
        MutableLong valueY = new MutableLong();
        CountDownLatch latch = new CountDownLatch(threadsCount);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicLong expectedCounter = new AtomicLong();

        // Submit writers
        for (int i = 0; i < threadsCount / 2; ++i) {
            executor.submit(() -> {
                try {
                    int counter = 0;
                    while (!stop.get()) {
                        lockManager.writeLock(lockAddress);
                        valueX.value++;
                        valueY.value++;
                        lockManager.releaseLock(lockAddress);
                        counter++;
                    }
                    expectedCounter.getAndAdd(counter);
                    latch.countDown();
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
        }

        // Submit readers
        for (int i = 0; i < threadsCount / 2; ++i) {
            executor.submit(() -> {
                try {
                    while (!stop.get()) {
                        long valX;
                        long valY;
                        lockManager.readLock(lockAddress);
                        valX = valueX.value;
                        valY = valueY.value;
                        lockManager.releaseLock(lockAddress);
                        assertEquals(valX, valY);
                    }
                    latch.countDown();
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
        }

        sleepAndStop(stop, runningTimeSeconds);
        assertOpenEventually(latch);
        assertNull(exception.get());
        lockManager.readLock(lockAddress);
        assertEquals(expectedCounter.get(), valueX.value);
        assertTrue(expectedCounter.get() > 0);
        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        executor.shutdownNow();
    }

    @Test(timeout = 600000)
    public void testReadWriteLocksMix() {
        int threadsCount = 4 * Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        AtomicBoolean stop = new AtomicBoolean();

        CountDownLatch latch = new CountDownLatch(threadsCount);
        AtomicReference<Throwable> exception = new AtomicReference<>();

        for (int i = 0; i < threadsCount; ++i) {
            executor.submit(() -> {
                try {
                    while (!stop.get()) {
                        int op = nextInt(5);
                        if (op == 0) {
                            lockManager.readLock(lockAddress);
                            lockManager.releaseLock(lockAddress);
                        } else if (op == 1) {
                            lockManager.writeLock(lockAddress);
                            lockManager.releaseLock(lockAddress);
                        } else if (op == 2) {
                            lockManager.instantDurationWriteLock(lockAddress);
                        } else if (op == 3) {
                            if (lockManager.tryWriteLock(lockAddress)) {
                                lockManager.releaseLock(lockAddress);
                            }
                        } else if (op == 4) {
                            lockManager.readLock(lockAddress);
                            lockManager.tryUpgradeToWriteLock(lockAddress);
                            lockManager.releaseLock(lockAddress);
                        }
                    }
                    latch.countDown();
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
        }

        sleepAndStop(stop, runningTimeSeconds);
        assertOpenEventually(latch);
        assertNull(exception.get());
        assertLockState(lockAddress, 0, 0, 0);

        executor.shutdownNow();
    }

    private long allocateZeroed(int size) {
        long address = defaultAllocator.allocate(size);
        AMEM.setMemory(address, size, (byte) 0);
        return address;
    }

    private int nextInt(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }

}
