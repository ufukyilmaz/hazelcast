package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.WORK_QUEUE_CAPACITY;
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Concurrent implementation of {@link HotRestartStore} which delegates work to a
 * single-threaded {@code HotRestartStore} over a concurrent queue.
 */
public final class ConcurrentHotRestartStore implements HotRestartStore {
    private static final IdleStrategy IDLER = new BackoffIdleStrategy(1, 1, 1, MICROSECONDS.toNanos(200));

    private final String name;
    private final HotRestartPersistenceEngine persistence;
    private final ConcurrentConveyorSingleQueue<RunnableWithStatus> persistenceConveyor;
    private final DiContainer di;
    private final PersistenceEngineLoop persistenceLoop;
    private final Thread persistenceThread;
    private final ILogger logger;

    @Inject
    private ConcurrentHotRestartStore(
            ILogger logger,
            @Name("storeName") String storeName,
            HotRestartPersistenceEngine persistence,
            @Name("persistenceConveyor") ConcurrentConveyorSingleQueue<RunnableWithStatus> persistenceConveyor,
            DiContainer di
    ) {
        this.logger = logger;
        this.name = storeName;
        this.persistence = persistence;
        this.persistenceConveyor = persistenceConveyor;
        this.di = di;
        this.persistenceLoop = new PersistenceEngineLoop();
        this.persistenceThread = new Thread(persistenceLoop, "Hot Restart Persistence Engine");
    }

    @Override
    public void hotRestart(
            boolean failIfAnyData, int storeCount, ConcurrentConveyor<RestartItem>[] keyConveyors,
            ConcurrentConveyor<RestartItem> keyHandleConveyor, ConcurrentConveyor<RestartItem>[] valueConveyors)
    throws InterruptedException {
        new DiContainer(di).dep(Rebuilder.class)
                           .dep("storeCount", storeCount)
                           .dep("keyConveyors", keyConveyors)
                           .dep("keyHandleConveyor", keyHandleConveyor)
                           .dep("valueConveyors", valueConveyors)
                           .instantiate(HotRestarter.class)
                           .restart(failIfAnyData);
        di.invoke(persistence, "start");
        persistenceThread.start();
    }

    @Override
    public void put(HotRestartKey key, byte[] value, boolean needsFsync) {
        submitAndProceedWhenAllowed(persistence.new Put(key, value, needsFsync));
    }

    @Override
    public void remove(HotRestartKey key, boolean needsFsync) {
        submitAndProceedWhenAllowed(persistence.new Remove(key, needsFsync));
    }

    @Override
    public void clear(boolean needsFsync, long... keyPrefixes) throws HotRestartException {
        submitAndProceedWhenAllowed(persistence.new Clear(keyPrefixes, needsFsync));
    }

    @Override
    public void close() throws HotRestartException {
        persistenceConveyor.submit(persistenceLoop.askToStop);
        try {
            persistenceThread.join();
        } catch (InterruptedException e) {
            throw new HotRestartException("Interrupted while waiting for the persistence engine to shut down", e);
        }
    }

    /**
     * Gives access to the internal persistence engine, exclusively for testing purposes.
     */
    public HotRestartPersistenceEngine getPersistenceEngine() {
        return persistence;
    }

    @Override
    public String name() {
        return name;
    }

    private void submitAndProceedWhenAllowed(RunnableWithStatus item) {
        persistenceConveyor.submit(item);
        for (long idleCount = 0; !item.submitterCanProceed; idleCount++) {
            IDLER.idle(idleCount);
            // FIRST establish that the drainer is gone and THEN that submitterCanProceed is still false.
            if (persistenceConveyor.isDrainerGone() && !item.submitterCanProceed) {
                // checkDrainerGone is now certain to throw an exception.
                persistenceConveyor.checkDrainerGone();
            }
        }
    }

    private final class PersistenceEngineLoop implements Runnable {

        boolean askedToStop;
        final RunnableWithStatus askToStop = new RunnableWithStatus(true) {
            @Override public void run() {
                askedToStop = true;
            }
        };

        @Override
        public void run() {
            final List<RunnableWithStatus> workDrain = new ArrayList<RunnableWithStatus>(WORK_QUEUE_CAPACITY);
            final List<RunnableWithStatus> blockingTasks = new ArrayList<RunnableWithStatus>(WORK_QUEUE_CAPACITY);
            final List<RunnableWithStatus> nonblockingTasks = new ArrayList<RunnableWithStatus>(WORK_QUEUE_CAPACITY);
            long drainCount = 0;
            long blockingItemCount = 0;
            try {
                long idleCount = 0;
                persistenceConveyor.drainerArrived();
                while (!askedToStop && !interrupted()) {
                    workDrain.clear();
                    persistenceConveyor.drainTo(workDrain);
                    if (workDrain.isEmpty()) {
                        IDLER.idle(idleCount++);
                    } else {
                        idleCount = 0;
                    }
                    blockingTasks.clear();
                    nonblockingTasks.clear();
                    for (RunnableWithStatus task : workDrain) {
                        (task.submitterCanProceed ? nonblockingTasks : blockingTasks).add(task);
                    }
                    if (!blockingTasks.isEmpty()) {
                        drainCount++;
                        blockingItemCount += blockingTasks.size();
                        for (Runnable task : blockingTasks) {
                            task.run();
                        }
                        persistence.fsync();
                        for (RunnableWithStatus task : blockingTasks) {
                            task.submitterCanProceed = true;
                        }
                    }
                    for (Runnable task : nonblockingTasks) {
                        task.run();
                    }
                }
                persistenceConveyor.drainerDone();
            } catch (Throwable t) {
                persistenceConveyor.drainerFailed(t);
            } finally {
                persistence.close();
                logger.fine(String.format("Drained %,d blocking items. Mean batch size was %.1f",
                        blockingItemCount, (double) blockingItemCount / drainCount));
            }
        }
    }
}
