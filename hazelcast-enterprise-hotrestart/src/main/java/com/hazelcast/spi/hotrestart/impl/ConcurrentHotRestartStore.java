package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;
import com.hazelcast.spi.hotrestart.impl.gc.BackupExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Concurrent implementation of {@link HotRestartStore} which delegates work to a
 * single-threaded {@link HotRestartPersistenceEngine} over a many-to-one concurrent queue.
 */
public final class ConcurrentHotRestartStore implements HotRestartStore {
    /** Capacity of the many-to-one queue used by partition threads to submit to HotRestart persistence thread */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MUTATOR_QUEUE_CAPACITY = 1 << 10;
    private static final IdleStrategy IDLER = new BackoffIdleStrategy(1, 1, 1, MICROSECONDS.toNanos(200));

    private final String name;
    private final HotRestartPersistenceEngine persistence;
    private final ConcurrentConveyorSingleQueue<RunnableWithStatus> persistenceConveyor;
    private final DiContainer di;
    private final PersistenceEngineLoop persistenceLoop;
    private final Thread persistenceThread;
    private final GcLogger logger;
    private final BackupExecutor backupExecutor;

    @Inject
    private ConcurrentHotRestartStore(
            GcLogger logger,
            @Name("storeName") String storeName,
            HotRestartPersistenceEngine persistence,
            @Name("persistenceConveyor") ConcurrentConveyorSingleQueue<RunnableWithStatus> persistenceConveyor,
            DiContainer di,
            BackupExecutor backupExecutor
    ) {
        this.logger = logger;
        this.name = storeName;
        this.persistence = persistence;
        this.persistenceConveyor = persistenceConveyor;
        this.di = di;
        this.persistenceLoop = new PersistenceEngineLoop();
        this.persistenceThread = new Thread(persistenceLoop, storeName + ".persistence-engine");
        this.backupExecutor = backupExecutor;
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
            currentThread().interrupt();
            throw new HotRestartException("Interrupted while waiting for the persistence engine to shut down", e);
        }
    }

    @Override
    public void backup(File targetDir) {
        if (!backupExecutor.prepareForNewTask()) {
            logger.fine("Another hot backup has already been run, aborting new backup");
            return;
        }
        submitAndProceedWhenAllowed(persistence.new Backup(targetDir));
    }

    @Override
    public BackupTaskState getBackupTaskState() {
        return backupExecutor.getBackupTaskState();
    }

    @Override
    public void interruptBackupTask() {
        backupExecutor.interruptBackupTask(false);
    }

    @Override
    public String name() {
        return name;
    }

    /** Gives access to the internal DI container, exclusively for testing purposes. */
    public DiContainer getDi() {
        return di;
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
        @SuppressWarnings("checkstyle:npathcomplexity")
        public void run() {
            final List<RunnableWithStatus> batch = new ArrayList<RunnableWithStatus>(MUTATOR_QUEUE_CAPACITY);
            final List<RunnableWithStatus> blockingTasks = new ArrayList<RunnableWithStatus>(MUTATOR_QUEUE_CAPACITY);
            final List<RunnableWithStatus> nonblockingTasks = new ArrayList<RunnableWithStatus>(MUTATOR_QUEUE_CAPACITY);
            long drainCount = 0;
            long blockingItemCount = 0;
            try {
                long idleCount = 0;
                persistenceConveyor.drainerArrived();
                while (!askedToStop && !interrupted()) {
                    batch.clear();
                    persistenceConveyor.drainTo(batch);
                    if (batch.isEmpty()) {
                        IDLER.idle(idleCount++);
                    } else {
                        idleCount = 0;
                    }
                    blockingTasks.clear();
                    nonblockingTasks.clear();
                    for (RunnableWithStatus task : batch) {
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
                try {
                    persistence.close();
                } catch (Throwable e) {
                    logger.severe("Hot restart engine failed to close", e);
                }
                logger.fine(String.format("Drained %,d blocking items. Mean batch size was %.1f",
                        blockingItemCount, (double) blockingItemCount / drainCount));
            }
        }
    }
}
