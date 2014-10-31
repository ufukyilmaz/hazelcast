package com.hazelcast.elastic.queue;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongConcurrentQueueTest {

    private static final int MAX_VALUE = 999999;
    private static final long NULL = MAX_VALUE + 1;

    private MemoryAllocator malloc;
    private LongQueue queue;

    @Before
    public void setUp() throws Exception {
        malloc = new StandardMemoryManager(new MemorySize(128, MemoryUnit.MEGABYTES));
    }

    @After
    public void tearDown() throws Exception {
        if (queue != null) {
            queue.destroy();
        }
    }

    @Test
    public void testConcurrentLinkedQueueProduceConsume() throws InterruptedException {
        queue = new LongConcurrentLinkedQueue(malloc, NULL);
        testProduceConsume();
    }

    @Test
    public void testArrayBlockingQueueProduceConsume() throws InterruptedException {
        queue = new LongArrayBlockingQueue(malloc, 10000, NULL);
        testProduceConsume();
    }

    @Test
    public void testLinkedBlockingQueueProduceConsume() throws InterruptedException {
        queue = new LongLinkedBlockingQueue(malloc, 10000, NULL);
        testProduceConsume();
    }

    private void testProduceConsume() throws InterruptedException {
        int pairs = 8;
        QueueWorker[] workers = new QueueWorker[pairs * 2];
        int ix = 0;
        for (int i = 0; i < pairs; i++) {
            Producer p = new Producer(queue);
            workers[ix++] = p;

            Consumer c = new Consumer(queue);
            workers[ix++] = c;

            p.start();
            c.start();
        }

        for (QueueWorker worker : workers) {
            Assert.assertTrue(worker.await(10, TimeUnit.MINUTES));
            Throwable error = worker.error;
            Assert.assertNull(toString(error), error);
        }
    }

    private static String toString(Throwable t) {
        if (t == null) {
            return "NULL";
        }
        StringWriter s = new StringWriter();
        t.printStackTrace(new PrintWriter(s));
        return s.toString();
    }

    private static abstract class QueueWorker extends Thread {
        static final int ITERATIONS = 50000;

        final LongQueue queue;
        final CountDownLatch latch = new CountDownLatch(1);
        Throwable error;

        protected QueueWorker(LongQueue queue) {
            this.queue = queue;
        }

        @Override
        public final void run() {
            try {
                for (int i = 0; i < ITERATIONS && error == null; i++) {
                    try {
                        runInternal();
                    } catch (NativeOutOfMemoryError e) {
                        LockSupport.parkNanos(1);
                    } catch (Throwable t) {
                        error = t;
                        break;
                    }
                }
            } finally {
                latch.countDown();
            }
        }

        boolean await(long time, TimeUnit unit) throws InterruptedException {
            return latch.await(time, unit);
        }

        protected abstract void runInternal();
    }

    private static class Producer extends QueueWorker {
        final Random rand = new Random();
        final boolean hasCapacity = queue.capacity() < Integer.MAX_VALUE;

        private Producer(LongQueue queue) {
            super(queue);
        }

        @Override
        public void runInternal() {
            long value = rand.nextInt(MAX_VALUE);
            boolean offered = queue.offer(value);
            if (!hasCapacity && !offered) {
                error = new AssertionError("Offer failed! -> " + queue);
            }
        }
    }

    private static class Consumer extends QueueWorker {

        private Consumer(LongQueue queue) {
            super(queue);
        }

        @Override
        public void runInternal() {
            long value = queue.poll();
            if (value == NULL) {
                LockSupport.parkNanos(1);
            } else if (value < 0 || value > MAX_VALUE) {
                error = new AssertionError("Invalid value: " + value);
            }
        }
    }

    @Test
    public void testConcurrentLinkedQueueDestroy() {
        queue = new LongConcurrentLinkedQueue(malloc, NULL);
        testDestroy();
    }

    @Test
    public void testArrayBlockingQueueDestroy() throws InterruptedException {
        queue = new LongArrayBlockingQueue(malloc, 10000, NULL);
        testDestroy();
    }

    @Test
    public void testLinkedBlockingQueueDestroy() throws InterruptedException {
        queue = new LongLinkedBlockingQueue(malloc, 10000, NULL);
        testDestroy();
    }

    private void testDestroy() {
        for (int i = 0; i < 10; i++) {
            queue.offer(System.nanoTime());
        }
        queue.destroy();
        queue.destroy();
        queue.destroy();
    }

    @Test(expected = IllegalStateException.class)
    public void testConcurrentLinkedQueueDestroyDuringProduce() {
        queue = new LongConcurrentLinkedQueue(malloc, NULL);
        testDestroyDuringProduce();
    }

    @Test(expected = IllegalStateException.class)
    public void testArrayBlockingQueueDestroyDuringProduce() throws InterruptedException {
        queue = new LongArrayBlockingQueue(malloc, 10000, NULL);
        testDestroyDuringProduce();
    }

    @Test(expected = IllegalStateException.class)
    public void testLinkedBlockingQueueDestroyDuringProduce() throws InterruptedException {
        queue = new LongLinkedBlockingQueue(malloc, 10000, NULL);
        testDestroyDuringProduce();
    }

    private void testDestroyDuringProduce() {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                queue.destroy();
            }
        }.start();

        while (true) {
            queue.offer(System.nanoTime());
            LockSupport.parkNanos(1);
            if (queue.size() >= 100) {
                latch.countDown();
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testConcurrentLinkedQueueDestroyDuringConsume() throws InterruptedException {
        queue = new LongConcurrentLinkedQueue(malloc, NULL);
        testDestroyDuringConsume();
    }

    @Test(expected = IllegalStateException.class)
    public void testArrayBlockingQueueDestroyDuringConsume() throws InterruptedException {
        queue = new LongArrayBlockingQueue(malloc, 10000, NULL);
        testDestroyDuringConsume();
    }

    @Test(expected = IllegalStateException.class)
    public void testLinkedBlockingQueueDestroyDuringConsume() throws InterruptedException {
        queue = new LongLinkedBlockingQueue(malloc, 10000, NULL);
        testDestroyDuringConsume();
    }

    private void testDestroyDuringConsume() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                queue.destroy();
            }
        }.start();

        for (int i = 0; i < 500; i++) {
            queue.offer(System.nanoTime());
        }

        while (!Thread.currentThread().isInterrupted()) {
            queue.poll();
            LockSupport.parkNanos(1);
            if (queue.size() <= 450) {
                latch.countDown();
            }
        }
    }
}
