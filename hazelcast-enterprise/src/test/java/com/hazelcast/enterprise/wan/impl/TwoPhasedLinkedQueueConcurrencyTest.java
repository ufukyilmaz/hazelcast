package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.enterprise.wan.impl.FinalizableInteger.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, NightlyTest.class})
public class TwoPhasedLinkedQueueConcurrencyTest {

    private static final long TEST_TIMEOUT_MILLIS = 10 * 60 * 1000; // 10 minutes
    private static final long TEST_DURATION_MILLIS = 10 * 1000; // 10 seconds
    private static final long LATCH_TIMEOUT_MILLIS = 10 * 1000; // 10 seconds
//    private static final long TEST_DURATION_MILLIS = 1 * 60 * 1000; // 1 minute

    /**
     * Test scenario: Running concurrent producers and consumers is thread-safe.
     * <p>
     * We start a few producer and a few consumer threads and we check after the
     * configured test duration that the sum of the produced and the consumed
     * integers are equal.
     */
    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentProducersAndConsumers() throws Exception {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        ExecutorService executorService = Executors.newCachedThreadPool();

        int cpus = Runtime.getRuntime().availableProcessors();
        int producers = cpus / 2;
        int consumers = cpus - producers;

        // we finalize the entries right on the consumer threads once they're dequeued
        TestControl control = new TestControl(queue, producers, consumers, executorService,
                FinalizableInteger::doFinalize);
        submitProducers(control);
        submitConsumers(control);

        control.startLatch.countDown();

        try {
            MILLISECONDS.sleep(TEST_DURATION_MILLIS);
        } finally {
            control.stopProducers.set(true);
        }

        long waitConsumersStart = System.nanoTime();
        control.consumersLatch.await(LATCH_TIMEOUT_MILLIS, MILLISECONDS);
        executorService.shutdownNow();
        System.out.println("Time spent on waiting for the consumers: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitConsumersStart) + "ms");

        assertEquals(control.expectedSum(), control.actualSum());
        assertQueueEmpty(queue);
    }

    /**
     * Test scenario: Consuming from the queue doesn't reorder the entries in the queue.
     * <p>
     * We start a single producer, and a few consumer and finalizer threads. After
     * the configured test duration elapsed, we check that the sum of the produced and
     * the consumed integers are equal. While the test is running, we periodically inspect
     * the queue and check if all entries are in the expected submission order: the
     * numbers in the queue are strictly monotonically increasing.
     */
    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentConsumersDontReorderEntries() throws Exception {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        ExecutorService executorService = Executors.newCachedThreadPool();

        int cpus = Runtime.getRuntime().availableProcessors();
        int producers = 1;
        int consumers = cpus;
        int finalizers = cpus;

        final Queue<FinalizableInteger> finalizableEntries = new LinkedBlockingQueue<>();
        AtomicBoolean pauseFinalizers = new AtomicBoolean();
        AtomicBoolean runFinalizer = new AtomicBoolean(true);

        // we don't finalize the queue entries right on the consumer threads
        // instead, we put them into a queue that is polled by pausable finalizer threads
        TestControl control = new TestControl(queue, producers, consumers, executorService, finalizableEntries::offer);
        submitProducers(control);
        submitConsumers(control);
        CountDownLatch finalizersLatch = new CountDownLatch(finalizers);

        // starting the finalizer threads
        for (int i = 0; i < finalizers; i++) {
            executorService.submit((Callable<Void>) () -> {
                try {
                    while (runFinalizer.get() || !finalizableEntries.isEmpty()
                            || control.consumersLatch.getCount() > 0) {

                        while (pauseFinalizers.get()) {
                            MILLISECONDS.sleep(100);
                        }

                        FinalizableInteger entry;
                        while ((entry = finalizableEntries.poll()) != null) {
                            entry.doFinalize();
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                } finally {
                    finalizersLatch.countDown();
                }

                return null;
            });
        }

        // starting the inspector thread
        // on this thread we periodically pause the producer, consumer and finalizer threads and
        // observe that the entries int the queue are in their original order
        executorService.submit((Callable<Void>) () -> {

            try {
                while (control.producersRunning.get() > 0 || control.queue.size() > 0) {
                    // ensuring there are some entries in the queue in every state
                    MILLISECONDS.sleep(100);
                    pauseFinalizers.set(true);
                    MILLISECONDS.sleep(100);
                    control.pauseConsumers.set(true);
                    MILLISECONDS.sleep(100);
                    control.pauseProducers.set(true);

                    AtomicInteger max = new AtomicInteger();
                    control.queue.consumeAll(n -> {
                        if (n.intValue() <= max.get()) {
                            control.consumedInOrder.set(false);
                        }
                        max.set(n.intValue());
                    });

                    pauseFinalizers.set(false);
                    control.pauseProducers.set(false);
                    control.pauseConsumers.set(false);
                }
            } catch (InterruptedException ex) {
            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            }

            return null;
        });

        control.startLatch.countDown();

        try {
            MILLISECONDS.sleep(TEST_DURATION_MILLIS);
        } finally {
            control.stopProducers.set(true);
            runFinalizer.set(false);
        }

        long waitConsumersStart = System.nanoTime();
        control.consumersLatch.await(LATCH_TIMEOUT_MILLIS, MILLISECONDS);
        finalizersLatch.await(LATCH_TIMEOUT_MILLIS, MILLISECONDS);
        executorService.shutdownNow();

        System.out.println("Time spent on waiting for the consumers: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitConsumersStart) + "ms");

        assertTrue(control.consumedInOrder.get());
        assertEquals(control.expectedSum(), control.actualSum());
        assertQueueEmpty(queue);
    }

    /**
     * Test scenario: Clearing the queue doesn't lead to lost entries. Every entry is
     * accounted either on the consumer threads or on the clear thread.
     * <p>
     * We start a a few producer and consumer threads and a single clear thread, making
     * the latter periodically clearing the queue. The producer this time always offer
     * constant 1, which we use for counting the events seen by consumer and the clear
     * thread. After the test duration elapsed we check the count of the produced entries
     * match to the sum of the consumed and cleared entries.
     */
    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentClear() throws Exception {
        TwoPhasedLinkedQueue<FinalizableInteger> queue = new TwoPhasedLinkedQueue<>();
        ExecutorService executorService = Executors.newCachedThreadPool();

        int cpus = Runtime.getRuntime().availableProcessors();
        int producers = cpus / 2;
        int consumers = cpus - producers;

        AtomicBoolean stopClearThread = new AtomicBoolean();
        AtomicInteger countCleared = new AtomicInteger();
        CountDownLatch clearLatch = new CountDownLatch(1);

        // we finalize the entries right on the consumer threads once they're dequeued
        // we publish constant 1
        TestControl control = new TestControl(queue, producers, consumers, executorService,
                FinalizableInteger::doFinalize, integer -> of(1));
        submitProducers(control);
        submitConsumers(control);

        executorService.submit((Callable<Void>) () -> {
            try {
                while ((!stopClearThread.get() || queue.size() > 0) && !Thread.currentThread().isInterrupted()) {
                    MILLISECONDS.sleep(100);

                    int cleared = queue.clear();
                    countCleared.addAndGet(cleared);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            }
            clearLatch.countDown();
            return null;
        });

        control.startLatch.countDown();

        try {
            MILLISECONDS.sleep(TEST_DURATION_MILLIS);
        } finally {
            control.stopProducers.set(true);
            control.producersLatch.await();
            stopClearThread.set(true);
        }


        long waitConsumersStart = System.nanoTime();
        control.consumersLatch.await(LATCH_TIMEOUT_MILLIS, MILLISECONDS);
        clearLatch.await(LATCH_TIMEOUT_MILLIS, MILLISECONDS);
        executorService.shutdownNow();
        System.out.println("Time spent on waiting for the consumers: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitConsumersStart) + "ms");

        assertTrue(control.consumedInOrder.get());
        assertEquals(control.expectedSum(), control.actualSum() + countCleared.get());
        assertQueueEmpty(queue);
    }

    private void submitProducers(TestControl control) {
        AtomicInteger slotSeq = new AtomicInteger();
        for (int i = 0; i < control.producers; i++) {
            control.executorService.submit((Callable<Void>) () -> {
                try {
                    ThreadLocalRandom random = ThreadLocalRandom.current();

                    int slot = slotSeq.getAndIncrement();
                    long num = slot;
                    long sum = 0;
                    control.startLatch.await();

                    while (!control.stopProducers.get()) {

                        control.pauseProducerIfRequested();

                        FinalizableInteger intOffered = control.intMapperFn.apply(FinalizableInteger.of((int) num));
                        control.queue.offer(intOffered);
                        sum += intOffered.intValue();
                        num += control.producers;

                        // giving a little air for the consumers since the producers are faster
                        if (control.queue.size() > 100_000) {
                            long rndNs = random.nextLong(200);
                            LockSupport.parkNanos(rndNs);
                        }
                    }

                    control.producersRunning.decrementAndGet();
                    control.expectedSums.set(slot, sum);
                    control.producersLatch.countDown();

                    return null;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            });
        }
    }

    private void submitConsumers(TestControl control) {
        AtomicInteger slotSeq = new AtomicInteger();
        for (int i = 0; i < control.consumers; i++) {
            control.executorService.submit((Callable<Void>) () -> {
                long sum = 0;
                control.startLatch.await();

                try {
                    while (control.producersRunning.get() > 0 || control.queue.size() > 0) {

                        control.pauseConsumerIfRequested();

                        FinalizableInteger entry = control.queue.dequeue();
                        if (entry != null) {
                            sum += entry.intValue();
                            control.queueEntryConsumer.accept(entry);
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                } finally {
                    control.actualSums.set(slotSeq.getAndIncrement(), sum);
                    control.consumersLatch.countDown();
                }
                return null;
            });
        }
    }

    private static void assertSumOfEntries(TwoPhasedLinkedQueue<FinalizableInteger> queue, long expectedSum) {
        AtomicLong actual = new AtomicLong();
        queue.consumeAll(e -> actual.addAndGet(e.intValue()));
        assertEquals(expectedSum, actual.get());
    }

    private static void assertQueueEmpty(TwoPhasedLinkedQueue<FinalizableInteger> queue) {
        assertSumOfEntries(queue, 0);
        assertEquals(0, queue.size());
    }

    private static final class TestControl {
        final int producers;
        final int consumers;
        final TwoPhasedLinkedQueue<FinalizableInteger> queue;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger producersRunning;
        final AtomicLongArray expectedSums;
        final AtomicLongArray actualSums;
        final CountDownLatch consumersLatch;
        final CountDownLatch producersLatch;
        final ExecutorService executorService;
        final AtomicBoolean consumedInOrder = new AtomicBoolean(true);
        final AtomicBoolean pauseProducers = new AtomicBoolean();
        final AtomicBoolean pauseConsumers = new AtomicBoolean();
        final AtomicBoolean stopProducers = new AtomicBoolean();
        final Consumer<FinalizableInteger> queueEntryConsumer;
        final Function<FinalizableInteger, FinalizableInteger> intMapperFn;

        private TestControl(TwoPhasedLinkedQueue<FinalizableInteger> queue, int producers, int consumers, ExecutorService executorService, Consumer<FinalizableInteger> queueEntryConsumer) {
            this(queue, producers, consumers, executorService, queueEntryConsumer, integer -> integer);
        }

        private TestControl(TwoPhasedLinkedQueue<FinalizableInteger> queue, int producers, int consumers, ExecutorService executorService, Consumer<FinalizableInteger> queueEntryConsumer, Function<FinalizableInteger, FinalizableInteger> intMapperFn) {
            this.producersRunning = new AtomicInteger(producers);
            this.expectedSums = new AtomicLongArray(producers);
            this.producers = producers;
            this.actualSums = new AtomicLongArray(consumers);
            this.producersLatch = new CountDownLatch(producers);
            this.consumersLatch = new CountDownLatch(consumers);

            this.queue = queue;
            this.consumers = consumers;
            this.executorService = executorService;
            this.queueEntryConsumer = queueEntryConsumer;
            this.intMapperFn = intMapperFn;
        }

        void pauseProducerIfRequested() throws InterruptedException {
            while (pauseProducers.get()) {
                MILLISECONDS.sleep(100);
            }
        }

        void pauseConsumerIfRequested() throws InterruptedException {
            while (pauseConsumers.get()) {
                MILLISECONDS.sleep(100);
            }
        }

        long expectedSum() {
            long expectedSum = 0;
            for (int i = 0; i < producers; i++) {
                expectedSum += expectedSums.get(i);
            }
            return expectedSum;
        }

        long actualSum() {
            long actualSum = 0;
            for (int i = 0; i < consumers; i++) {
                actualSum += actualSums.get(i);
            }
            return actualSum;
        }
    }
}
