package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkManager;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.GcMainLoop;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.PrefixTombstoneManager;
import com.hazelcast.spi.hotrestart.impl.gc.Snapshotter;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.util.concurrent.ManyToOneConcurrentArrayQueue;
import com.hazelcast.util.concurrent.OneToOneConcurrentArrayQueue;

import static com.hazelcast.spi.hotrestart.impl.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;

/**
 * Contains Hot Restart Store factory methods.
 */
public final class HotRestartModule {

    private HotRestartModule() { }

    /**
     * Constructs, configures, and returns an on-heap Hot Restart Store.
     * @param cfg the configuration object
     */
    public static HotRestartStore newOnHeapHotRestartStore(HotRestartStoreConfig cfg) {
        return hrStore(cfg, false);
    }

    /**
     * Constructs, configures, and returns an off-heap Hot Restart Store.
     * @param cfg the configuration object
     */
    public static HotRestartStore newOffHeapHotRestartStore(HotRestartStoreConfig cfg) {
        return hrStore(cfg, true);
    }

    private static HotRestartStore hrStore(HotRestartStoreConfig cfg, boolean isOffHeap) {
        cfg.logger().info(cfg.storeName() + " homeDir: " + cfg.homeDir());
        cfg.validateAndCreateHomeDir();
        final DiContainer di = new DiContainer();
        if (isOffHeap) {
            final MemoryAllocator malloc = cfg.malloc();
            if (malloc == null) {
                throw new IllegalArgumentException("cfg.malloc is null");
            }
            di.dep(MemoryAllocator.class, malloc);
        }
        di.dep(di)
          .dep("storeName", cfg.storeName())
          .dep("homeDir", cfg.homeDir())
          .dep(RamStoreRegistry.class, cfg.ramStoreRegistry())
          .dep(MetricsRegistry.class, cfg.metricsRegistry())
          .dep(ILogger.class, cfg.logger())

          .dep("testGcMutex", new Object())
          .dep(new RecordDataHolder())
          .dep(GcLogger.class)
          .dep(GcHelper.class, isOffHeap ? GcHelper.OffHeap.class : GcHelper.OnHeap.class).disposable()
          .dep("persistenceConveyor", concurrentConveyorSingleQueue(null,
                  new ManyToOneConcurrentArrayQueue<Runnable>(GcExecutor.WORK_QUEUE_CAPACITY)))
          .dep("gcConveyor", concurrentConveyorSingleQueue(null,
                  new OneToOneConcurrentArrayQueue<Runnable>(GcExecutor.WORK_QUEUE_CAPACITY)))
          .dep(ChunkManager.class).disposable()
          .dep(Snapshotter.class)
          .dep(MutatorCatchup.class)
          .dep(PrefixTombstoneManager.class)
          .dep(GcMainLoop.class)
          .dep(GcExecutor.class)
          .dep(HotRestartPersistenceEngine.class)
        ;
        di.wireAndInitializeAll();
        return di.instantiate(ConcurrentHotRestartStore.class);
    }

}
