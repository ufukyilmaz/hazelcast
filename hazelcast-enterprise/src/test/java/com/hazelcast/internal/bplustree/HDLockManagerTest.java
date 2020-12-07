package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDLockManagerTest extends HazelcastTestSupport {

    private static final int THREADS_COUNT = Runtime.getRuntime().availableProcessors();

    @Parameterized.Parameters(name = "writeWaiterWinsPercentage:{0}, readLockRequestWinsPercentage: {1}, writeLockRequestWinsPercentage: {2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {-1, -1, -1}, // use default policy
                {100.0f, 100.0f, 100.0f}, // max throughput, write waiter priority
                {70.0f, 100.0f, 100.0f}, // max throughput, less write waiter priority
                {0.0f, 100.0f, 100.0f},  // max throughput, read waiter priority
                {100.0f, 0.0f, 0.0f}, // fairness, min throughput, write waiter priority
                {70.0f, 0.0f, 0.0f}, // fairness, min throughput, less write waiter priority
                {0.0f, 0.0f, 0.0f}, // fairness, min throughput, read waiter priority
                {70.0f, 30.0f, 70.0f}, // average throughput favouring writer
        });
    }

    @Parameterized.Parameter(0)
    public float writeWaiterWinsPercentage;

    @Parameterized.Parameter(1)
    public float readLockRequestWinsPercentage;

    @Parameterized.Parameter(2)
    public float writeLockRequestWinsPercentage;

    private HDLockManager lockManager;
    private long lockAddress;
    private ExecutorService es;
    private HazelcastMemoryManager defaultAllocator;

    @Before
    public void setUp() {
        defaultAllocator = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        LockFairnessPolicy fairnessPolicy;
        if (writeLockRequestWinsPercentage == -1) {
            fairnessPolicy = new DefaultLockFairnessPolicy();
        } else {
            fairnessPolicy = getLockFairnessPolicy(writeWaiterWinsPercentage,
                    readLockRequestWinsPercentage, writeLockRequestWinsPercentage);
        }
        int stripesCount = 32;
        lockManager = new HDLockManager(stripesCount, fairnessPolicy);
        lockAddress = allocateZeroed(8);
        es = Executors.newFixedThreadPool(THREADS_COUNT);
    }

    @After
    public void tearDown() {
        if (es != null) {
            es.shutdownNow();
        }
        if (defaultAllocator != null) {
            defaultAllocator.dispose();
        }
    }

    @Test
    public void testReadLock() {
        lockManager.readLock(lockAddress);

        assertLockState(lockAddress, 1, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        lockManager.readLock(lockAddress);
        lockManager.readLock(lockAddress);
        lockManager.readLock(lockAddress);

        assertLockState(lockAddress, 3, 0, 0);
        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 2, 0, 0);
        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 1, 0, 0);
        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testTryReadLock() {
        assertTrue(lockManager.tryReadLock(lockAddress));

        assertLockState(lockAddress, 1, 0, 0);

        assertTrue(lockManager.tryReadLock(lockAddress));

        assertLockState(lockAddress, 2, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 1, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        lockManager.writeLock(lockAddress);
        assertFalse(lockManager.tryReadLock(lockAddress));
        assertLockState(lockAddress, -1, 0, 0);
    }

    @Test
    public void testInstantDurationReadLock() {
        lockManager.instantDurationReadLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        lockManager.writeLock(lockAddress);

        CountDownLatch latch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.instantDurationReadLock(lockAddress);
            latch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 1, 0));

        lockManager.releaseLock(lockAddress);

        assertOpenEventually(latch);
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testTryWriteLock() {
        assertTrue(lockManager.tryWriteLock(lockAddress));

        assertLockState(lockAddress, -1, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        lockManager.readLock(lockAddress);
        assertFalse(lockManager.tryWriteLock(lockAddress));
        assertLockState(lockAddress, 1, 0, 0);
    }

    @Test
    public void testWriteLock() {
        lockManager.writeLock(lockAddress);
        for (int i = 0; i < THREADS_COUNT; ++i) {
            es.submit(() -> {
                lockManager.writeLock(lockAddress);
            });
        }

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 0, THREADS_COUNT));

        for (int i = THREADS_COUNT; i > 0; --i) {
            lockManager.releaseLock(lockAddress);
            int expectedUsersCount = -1;
            int expectedWriteWaitersCount = i - 1;
            assertTrueEventually(() -> assertLockState(lockAddress, expectedUsersCount, 0, expectedWriteWaitersCount));
        }
    }

    @Test
    public void testReadWriteLock() {
        lockManager.writeLock(lockAddress);
        assertLockState(lockAddress, -1, 0, 0);

        assertFalse(lockManager.tryWriteLock(lockAddress));

        CountDownLatch latch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.readLock(lockAddress);
            latch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 1, 0));

        lockManager.releaseLock(lockAddress);
        assertOpenEventually(latch);

        assertLockState(lockAddress, 1, 0, 0);

        CountDownLatch latch2 = new CountDownLatch(2);
        es.submit(() -> {
            lockManager.writeLock(lockAddress);
            latch2.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, 1, 0, 1));

        es.submit(() -> {
            lockManager.writeLock(lockAddress);
            latch2.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, 1, 0, 2));

        lockManager.releaseLock(lockAddress);

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 0, 1));

        lockManager.releaseLock(lockAddress);
        assertOpenEventually(latch2);

        assertLockState(lockAddress, -1, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testUpgradeLock() {
        lockManager.readLock(lockAddress);
        assertLockState(lockAddress, 1, 0, 0);

        assertTrue(lockManager.tryUpgradeToWriteLock(lockAddress));
        assertLockState(lockAddress, -1, 0, 0);

        lockManager.releaseLock(lockAddress);

        assertLockState(lockAddress, 0, 0, 0);

        lockManager.readLock(lockAddress);
        lockManager.readLock(lockAddress);
        assertLockState(lockAddress, 2, 0, 0);

        assertFalse(lockManager.tryUpgradeToWriteLock(lockAddress));
        assertLockState(lockAddress, 2, 0, 0);

        lockManager.releaseLock(lockAddress);
        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testInstantDurationWriteLock() {
        assumeTrue(fullPercentage(readLockRequestWinsPercentage));
        lockManager.instantDurationWriteLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);

        lockManager.readLock(lockAddress);

        CountDownLatch latch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.instantDurationWriteLock(lockAddress);
            latch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, 1, 0, 1));

        lockManager.readLock(lockAddress);

        assertLockState(lockAddress, 2, 0, 1);

        lockManager.releaseLock(lockAddress);
        lockManager.releaseLock(lockAddress);

        assertTrueEventually(() -> assertLockState(lockAddress, 0, 0, 0));
    }

    @Test
    public void testWriteLockSignallPriority() {
        assumeTrue(fullPercentage(writeWaiterWinsPercentage));
        lockManager.writeLock(lockAddress);

        assertLockState(lockAddress, -1, 0, 0);

        CountDownLatch readLatch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.readLock(lockAddress);
            readLatch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 1, 0));

        CountDownLatch writeLatch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.writeLock(lockAddress);
            writeLatch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 1, 1));

        lockManager.releaseLock(lockAddress);

        assertOpenEventually(writeLatch);
        assertClosed(readLatch);
        assertLockState(lockAddress, -1, 1, 0);

        lockManager.releaseLock(lockAddress);

        assertOpenEventually(readLatch);
        assertLockState(lockAddress, 1, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testReadLockSignallPriority() {
        assumeTrue(writeWaiterWinsPercentage == 0);
        lockManager.writeLock(lockAddress);

        assertLockState(lockAddress, -1, 0, 0);

        CountDownLatch readLatch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.readLock(lockAddress);
            readLatch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 1, 0));

        CountDownLatch writeLatch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.writeLock(lockAddress);
            writeLatch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, -1, 1, 1));

        lockManager.releaseLock(lockAddress);

        assertOpenEventually(readLatch);
        assertClosed(writeLatch);
        assertLockState(lockAddress, 1, 0, 1);

        lockManager.releaseLock(lockAddress);

        assertOpenEventually(writeLatch);
        assertLockState(lockAddress, -1, 0, 0);

        lockManager.releaseLock(lockAddress);
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testReadLockRequestPriority() {
        assumeTrue(fullPercentage(readLockRequestWinsPercentage));
        lockManager.readLock(lockAddress);

        assertLockState(lockAddress, 1, 0, 0);

        CountDownLatch writeLatch = new CountDownLatch(1);
        es.submit(() -> {
            lockManager.writeLock(lockAddress);
            writeLatch.countDown();
        });

        assertTrueEventually(() -> assertLockState(lockAddress, 1, 0, 1));

        lockManager.readLock(lockAddress);
        assertTrueEventually(() -> assertLockState(lockAddress, 2, 0, 1));
        lockManager.readLock(lockAddress);
        assertTrueEventually(() -> assertLockState(lockAddress, 3, 0, 1));

        lockManager.releaseLock(lockAddress);
        assertTrueEventually(() -> assertLockState(lockAddress, 2, 0, 1));
        lockManager.releaseLock(lockAddress);
        assertTrueEventually(() -> assertLockState(lockAddress, 1, 0, 1));
        lockManager.releaseLock(lockAddress);
        assertTrueEventually(() -> assertLockState(lockAddress, -1, 0, 0));
        lockManager.releaseLock(lockAddress);
        assertTrueEventually(() -> assertLockState(lockAddress, 0, 0, 0));
    }

    @Test
    public void testIllegalUse() {
        assertThrows(AssertionError.class, () -> lockManager.releaseLock(lockAddress));

        assertLockState(lockAddress, 0, 0, 0);

        lockManager.readLock(lockAddress);
        lockManager.releaseLock(lockAddress);

        assertThrows(AssertionError.class, () -> lockManager.releaseLock(lockAddress));
        assertLockState(lockAddress, 0, 0, 0);

        // Try upgrade without read lock
        assertThrows(AssertionError.class, () -> lockManager.tryUpgradeToWriteLock(lockAddress));
        assertLockState(lockAddress, 0, 0, 0);

        // Try upgrade write lock
        lockManager.writeLock(lockAddress);
        assertThrows(AssertionError.class, () -> lockManager.tryUpgradeToWriteLock(lockAddress));
        lockManager.releaseLock(lockAddress);
        assertThrows(AssertionError.class, () -> lockManager.releaseLock(lockAddress));
    }

    @Test
    public void testConcurrency() {
        int threadsCount = Runtime.getRuntime().availableProcessors();
        CountDownLatch latch = new CountDownLatch(threadsCount);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        for (int i = 0; i < threadsCount; ++i) {
            es.submit(() -> {
                try {
                    for (int j = 0; j < 10000; ++j) {
                        boolean writeLock = nextBoolean();
                        if (writeLock) {
                            lockManager.writeLock(lockAddress);
                        } else {
                            lockManager.readLock(lockAddress);
                        }
                        lockManager.releaseLock(lockAddress);
                    }
                    latch.countDown();
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
        }
        assertOpenEventually(latch);
        assertNull(exception.get());
        assertLockState(lockAddress, 0, 0, 0);
    }

    @Test
    public void testConcurrency2() {
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int threadsCount = Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        MutableLong value = new MutableLong();
        int lockRequestsPerThreadCount = 10000;
        CountDownLatch latch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; ++i) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < lockRequestsPerThreadCount; ++j) {
                        lockManager.writeLock(lockAddress);
                        value.value++;
                        lockManager.releaseLock(lockAddress);
                    }
                    latch.countDown();
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
        }
        assertOpenEventually(latch);
        assertNull(exception.get());
        lockManager.readLock(lockAddress);
        assertEquals(threadsCount * lockRequestsPerThreadCount, value.value);
        lockManager.releaseLock(lockAddress);
        executor.shutdownNow();
    }

    @Test
    public void testInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new HDLockManager(-1));
        assertThrows(IllegalArgumentException.class, () -> new HDLockManager(5));
        assertThrows(IllegalArgumentException.class, () -> new HDLockManager(100));
        assertThrows(IllegalArgumentException.class,
                () -> new HDLockManager(32, getLockFairnessPolicy(-1, 0, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> new HDLockManager(32, getLockFairnessPolicy(0, -1, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> new HDLockManager(32, getLockFairnessPolicy(0, 0, -1)));
        assertThrows(IllegalArgumentException.class,
                () -> new HDLockManager(32, getLockFairnessPolicy(101, 0, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> new HDLockManager(32, getLockFairnessPolicy(0, 101, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> new HDLockManager(32, getLockFairnessPolicy(0, 0, 101)));
    }

    @Test
    public void testExceedUsersCountLimit() {

        // Reach maximum number of lock users
        for (int i = 0; i < Short.MAX_VALUE; ++i) {
            lockManager.readLock(lockAddress);
        }

        assertLockState(lockAddress, Short.MAX_VALUE, 0, 0);

        // Overflow the user's count
        assertThrows(BPlusTreeLimitException.class, () -> lockManager.readLock(lockAddress));

        assertLockState(lockAddress, Short.MAX_VALUE, 0, 0);
    }

    @Test
    public void testExceedReadWriteWaitersLimit() {

        lockManager.writeLock(lockAddress);

        assertLockState(lockAddress,
                -1, 0, 0);

        // Simulate the maximum number of read and write waiters
        updateWaitersCount(lockAddress, true, 0xFFFF);
        updateWaitersCount(lockAddress, false, 0xFFFF);

        assertLockState(lockAddress,
                -1, 0xFFFF, 0xFFFF);

        // Overflow the read waiters
        assertThrows(BPlusTreeLimitException.class, () -> lockManager.readLock(lockAddress));

        assertLockState(lockAddress,
                -1, 0xFFFF, 0xFFFF);

        // Overflow the write waiters
        assertThrows(BPlusTreeLimitException.class, () -> lockManager.writeLock(lockAddress));

        assertLockState(lockAddress,
                -1, 0xFFFF, 0xFFFF);
    }

    static void assertLockState(long lockAddress,
                                int expectedUsersCount, int expectedReadWaitersCount, int expectedWriteWaitersCount) {
        long lockState = AMEM.getLongVolatile(lockAddress);
        long mask = 0xFFFFL;
        assertEquals(expectedUsersCount, (short) (lockState & mask));
        assertEquals(expectedReadWaitersCount, (lockState >> 16) & mask);
        assertEquals(expectedWriteWaitersCount, (lockState >> 32) & mask);
    }

    void assertClosed(CountDownLatch latch) {
        assertTrue(latch.getCount() > 0);
    }

    private long allocateZeroed(int size) {
        long address = defaultAllocator.allocate(size);
        AMEM.setMemory(address, size, (byte) 0);
        return address;
    }

    static boolean nextBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    private void updateWaitersCount(long lockAddr, boolean sharedAccess, int delta) {
        long lockState = AMEM.getLong(lockAddr);
        int shiftBits = sharedAccess ? 16 : 32;
        long clearingMask = sharedAccess ? 0xFFFFFFFF0000FFFFL : 0xFFFF0000FFFFFFFFL;
        int waitersCount = (int) ((lockState >> shiftBits) & 0xFFFFL);
        long newWaitersCount = waitersCount + delta;
        long newLockState = (lockState & clearingMask) | newWaitersCount << shiftBits;
        AMEM.putLong(lockAddress, newLockState);
    }

    private LockFairnessPolicy getLockFairnessPolicy(float writeWaiterWinsPercentage, float readLockRequestWinsPercentage,
                                                     float writeLockRequestWinsPercentage) {
        return new LockFairnessPolicy() {
            @Override
            public float writeWaiterWinsPercentage() {
                return writeWaiterWinsPercentage;
            }

            @Override
            public float readLockRequestWinsPercentage() {
                return readLockRequestWinsPercentage;
            }

            @Override
            public float writeLockRequestWinsPercentage() {
                return writeLockRequestWinsPercentage;
            }
        };
    }

    private static boolean fullPercentage(float percentage) {
        return percentage == 100.0f;
    }

}
