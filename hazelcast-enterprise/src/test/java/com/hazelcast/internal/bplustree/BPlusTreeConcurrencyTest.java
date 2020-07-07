package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeConcurrencyTest extends BPlusTreeTestSupport {

    @Parameterized.Parameter
    public int nodeSize;

    @Parameterized.Parameters(name = "nodeSize:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {256},
                {512},
                {1024},
                {2048},
                {4096},
                {8192}
        });
    }

    private static final int THREADS_COUNT = 5;
    private ExecutorService executor;

    @Override
    int getNodeSize() {
        return nodeSize;
    }

    @Before
    public void setUp() {
        super.setUp();
        executor = Executors.newFixedThreadPool(THREADS_COUNT);
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    DelegatingMemoryAllocator newDelegatingIndexMemoryAllocator(MemoryAllocator indexAllocator, AllocatorCallback callback) {
        // Don't pass a callback which might be not thread-safe
        return new DelegatingMemoryAllocator(indexAllocator, null);
    }

    @Test
    public void testInsertKeysConcurrently() {
        int keysPerThread = 10000;
        CountDownLatch latch = new CountDownLatch(THREADS_COUNT);
        AtomicReference<Throwable> exception = new AtomicReference<>();

        for (int i = 0; i < THREADS_COUNT; ++i) {
            int index = i;
            executor.submit(() -> {
                int startingIndex = index * keysPerThread;
                try {
                    for (int n = 0; n < keysPerThread; ++n) {
                        insertKey(startingIndex + n);
                    }
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);
        assertNull(exception.get());
        assertEquals(keysPerThread * THREADS_COUNT, queryKeysCount());
    }

    @Test
    public void testRemoveKeysConcurrently() {
        int keysCount = 10000;

        for (int i = 0; i < keysCount; ++i) {
            insertKey(i);
        }
        CountDownLatch latch = new CountDownLatch(THREADS_COUNT);
        AtomicReference<Throwable> exception = new AtomicReference<>();

        for (int i = 0; i < THREADS_COUNT; ++i) {
            executor.submit(() -> {
                int startIndex = nextInt(keysCount);
                try {
                    for (int j = startIndex; j < keysCount; ++j) {
                        removeKey(j);
                    }
                    for (int j = startIndex; j < keysCount; ++j) {
                        insertKey(j);
                    }
                } catch (Throwable t) {
                    t.printStackTrace(System.err);
                    exception.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        assertEquals(keysCount, queryKeysCount());
    }

    @Test
    public void testIteratorMonotonicityOnInsert() {
        int keysCount = 100000;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);

        // Submit inserter
        executor.submit(() -> {
                    try {
                        for (int i = 0; i < keysCount; ++i) {
                            insertKey(i);
                        }
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                    } finally {
                        latch.countDown();
                    }
                }
        );

        // Submit reader
        executor.submit(() -> {
            try {
                int count = 0;
                int prevCount;
                while (count != keysCount) {
                    prevCount = count;
                    count = queryKeysCount();
                    assertTrue(prevCount <= count);
                }
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            } finally {
                latch.countDown();
            }
        });

        assertOpenEventually(latch);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        assertEquals(keysCount, queryKeysCount());
    }

    @Test
    public void testIteratorMonotonicityOnRemove() {
        int keysCount = 100000;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);

        for (int i = 0; i < keysCount; ++i) {
            insertKey(i);
        }

        // Submit inserter
        executor.submit(() -> {
                    try {
                        for (int i = 0; i < keysCount; ++i) {
                            removeKey(i);
                        }
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                    } finally {
                        latch.countDown();
                    }
                }
        );

        // Submit reader
        executor.submit(() -> {
            try {
                int count = Integer.MAX_VALUE;
                int prevCount;
                while (count > 0) {
                    prevCount = count;
                    count = queryKeysCount();
                    assertTrue(prevCount >= count);
                }
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            } finally {
                latch.countDown();
            }
        });

        assertOpenEventually(latch);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        assertEquals(0, queryKeysCount());
    }

    @Test
    public void testIteratorHasNoDuplicates() {
        int keysCount = 100000;

        for (int i = 0; i < keysCount; ++i) {
            insertKey(i);
        }
        CountDownLatch latch = new CountDownLatch(THREADS_COUNT);
        AtomicReference<Throwable> exception = new AtomicReference<>();

        for (int i = 0; i < THREADS_COUNT; ++i) {
            boolean updater = i % 2 == 0;
            executor.submit(() -> {
                int startIndex = nextInt(keysCount);
                try {
                    if (updater) {
                        for (int j = startIndex; j < keysCount; ++j) {
                            removeKey(j);
                        }
                        for (int j = startIndex; j < keysCount; ++j) {
                            insertKey(j);
                        }
                    } else {
                        Set<String> mapKeys = new HashSet<>();
                        Iterator<QueryableEntry> it = btree.lookup(startIndex, true, null, true);
                        int count = 0;
                        while (it.hasNext()) {
                            QueryableEntry<String, String> entry = it.next();
                            String mapKey = entry.getKey();
                            if (mapKeys.contains(mapKey)) {
                                fail("Duplicate mapKey " + mapKey);
                            }
                            mapKeys.add(entry.getKey());
                            count++;
                        }
                        assertTrue(count <= keysCount - startIndex);
                    }
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        assertEquals(keysCount, queryKeysCount());
    }

    @Test
    public void testInsertRemove() {
        int keysCount = 100000;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);

        // Submit inserter
        executor.submit(() -> {
                    try {
                        for (int i = 0; i < keysCount; ++i) {
                            insertKey(i);
                        }
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                    } finally {
                        latch.countDown();
                    }
                }
        );

        // Submit reader
        executor.submit(() -> {
            try {
                for (int i = 0; i < keysCount; ++i) {

                    for (; ; ) {
                        Data oldValue = removeKey(i);
                        if (oldValue != null) {
                            break;
                        }
                        Thread.sleep(1);
                    }

                }
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            } finally {
                latch.countDown();
            }
        });

        assertOpenEventually(latch);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        assertEquals(0, queryKeysCount());
    }

    @Test
    public void testIterationAndRemove() {
        int keysCount = 100000;
        int portionCount = 100;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);
        for (int i = 0; i < keysCount; ++i) {
            insertKey(i);
        }

        // Submit remover
        executor.submit(() -> {
                    try {
                        int startIndex = nextInt(keysCount);
                        for (int i = 0; i < 1000; ++i) {
                            for (int j = startIndex; j < startIndex + portionCount; ++j) {
                                removeKey(j);
                            }

                            for (int j = startIndex; j < Math.min(startIndex + portionCount, keysCount); ++j) {
                                insertKey(j);
                            }
                        }
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                    } finally {
                        latch.countDown();
                    }
                }
        );

        // Submit reader
        executor.submit(() -> {
            try {
                for (int i = 0; i < 500; ++i) {
                    int count = queryKeysCount();
                    assertTrue(count <= keysCount);
                    assertTrue(count >= keysCount - portionCount);
                }
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            } finally {
                latch.countDown();
            }
        });

        assertOpenEventually(latch);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        assertEquals(keysCount, queryKeysCount());
    }

    @Test
    public void testRemoveInnerKeysFromOtherThread() {
        int keysCount = 50000;
        insertKeys(keysCount);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                for (int i = 0; i < keysCount; ++i) {
                    removeKey(i);
                }
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
                t.printStackTrace(System.err);
            } finally {
                latch.countDown();
            }
        });

        assertOpenEventually(latch);

        assertNull(exception.get());

        assertEquals(0, queryKeysCount());
    }

}
