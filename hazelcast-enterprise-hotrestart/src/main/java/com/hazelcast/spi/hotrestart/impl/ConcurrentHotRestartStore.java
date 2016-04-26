package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.ConcurrentConveyor.IDLER;
import static com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.WORK_QUEUE_CAPACITY;
import static java.lang.Thread.interrupted;

/**
 * Concurrent implementation of {@link HotRestartStore} which delegates work to a
 * single-threaded {@code HotRestartStore} over a concurrent queue.
 */
public final class ConcurrentHotRestartStore implements HotRestartStore {
    private final HotRestartPersistenceEngine persistence;
    private final ConcurrentConveyorSingleQueue<RunnableWithStatus> persistenceConveyor;
    private final DiContainer di;
    private final PersistenceEngineLoop persistenceLoop;
    private final Thread persistenceThread;

    @Inject
    private ConcurrentHotRestartStore(
            HotRestartPersistenceEngine persistence,
            @Name("persistenceConveyor") ConcurrentConveyorSingleQueue<RunnableWithStatus> persistenceConveyor,
            DiContainer di
    ) {
        this.persistence = persistence;
        this.persistenceConveyor = persistenceConveyor;
        this.di = di;
        this.persistenceLoop = new PersistenceEngineLoop();
        this.persistenceThread = new Thread(persistenceLoop, "Hot Restart Persistence Engine");
    }

    @Override
    public void hotRestart(
            boolean failIfAnyData, ConcurrentConveyor<RestartItem>[] keyConveyors,
            ConcurrentConveyor<RestartItem> keyHandleConveyor, ConcurrentConveyor<RestartItem>[] valueConveyors)
    throws InterruptedException {
        new DiContainer(di).dep(Rebuilder.class)
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
        persistenceConveyor.submit(persistence.new Put(key, value, needsFsync));
    }

    @Override
    public void remove(HotRestartKey key, boolean needsFsync) {
        persistenceConveyor.submit(persistence.new Remove(key, needsFsync));
    }

    @Override
    public void clear(boolean needsFsync, long... keyPrefixes) throws HotRestartException {
        persistenceConveyor.submit(persistence.new Clear(keyPrefixes, needsFsync));
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

    private final class PersistenceEngineLoop implements Runnable {

        boolean askedToStop;
        final RunnableWithStatus askToStop = new RunnableWithStatus(true) {
            @Override public void run() {
                askedToStop = true;
            }
        };

        private final List<RunnableWithStatus> workDrain = new ArrayList<RunnableWithStatus>(WORK_QUEUE_CAPACITY);
        private final List<RunnableWithStatus> blockingTasks = new ArrayList<RunnableWithStatus>(WORK_QUEUE_CAPACITY);
        private final List<RunnableWithStatus> nonblockingTasks = new ArrayList<RunnableWithStatus>(WORK_QUEUE_CAPACITY);

        @Override
        public void run() {
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
            }
        }
    }
}
