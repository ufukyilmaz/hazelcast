package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.concurrent.ManyToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.gc.BackupExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkManager;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.GcMainLoop;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.PrefixTombstoneManager;
import com.hazelcast.spi.hotrestart.impl.gc.Snapshotter;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.io.TombFileAccessor;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.util.ExceptionUtil;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static com.hazelcast.spi.hotrestart.impl.ConcurrentHotRestartStore.MUTATOR_QUEUE_CAPACITY;
import static com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.COLLECTOR_QUEUE_CAPACITY;

/**
 * Contains Hot Restart Store factory methods.
 */
public final class HotRestartModule {

    private HotRestartModule() { }

    /**
     * Constructs, configures, and returns an on-heap Hot Restart Store.
     *
     * @param cfg        the configuration object
     * @param properties hazelcast configuration properties
     */
    public static HotRestartStore newOnHeapHotRestartStore(HotRestartStoreConfig cfg, HazelcastProperties properties) {
        return hrStore(cfg, false, properties);
    }

    /**
     * Constructs, configures, and returns an off-heap Hot Restart Store.
     *
     * @param cfg        the configuration object
     * @param properties hazelcast configuration properties
     */
    public static HotRestartStore newOffHeapHotRestartStore(HotRestartStoreConfig cfg, HazelcastProperties properties) {
        return hrStore(cfg, true, properties);
    }

    private static HotRestartStore hrStore(HotRestartStoreConfig cfg, boolean isOffHeap, HazelcastProperties properties) {
       // fail fast when the explicit DirectByteBuffer clean-up is not available (i.e. TombFileAccessor.<clinit> fails)
        try {
            Class.forName(TombFileAccessor.class.getName());
        } catch (ClassNotFoundException e) {
            throw ExceptionUtil.rethrow(e);
        } catch (ExceptionInInitializerError e) {
            throw ExceptionUtil.rethrow(e.getCause() != null ? e.getCause() : e);
        }
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
        Class<? extends GcHelper> gcHelperClass = isOffHeap ? GcHelper.OffHeap.class : GcHelper.OnHeap.class;
        di.dep(di)
          .dep(HazelcastProperties.class, properties)
          .dep(new BackupExecutor()).disposable()
          .dep("storeName", cfg.storeName())
          .dep("homeDir", cfg.homeDir())
          .dep(RamStoreRegistry.class, cfg.ramStoreRegistry())
          .dep(MetricsRegistry.class, cfg.metricsRegistry())
          .dep(ILogger.class, cfg.logger())

          .dep("testGcMutex", new Object())
          .dep(new RecordDataHolder())
          .dep(GcLogger.class)
          .dep(GcHelper.class, gcHelperClass).disposable()
          .dep("persistenceConveyor", concurrentConveyorSingleQueue(null,
                  new ManyToOneConcurrentArrayQueue<Runnable>(MUTATOR_QUEUE_CAPACITY)))
          .dep("gcConveyor", concurrentConveyorSingleQueue(null,
                  new OneToOneConcurrentArrayQueue<Runnable>(COLLECTOR_QUEUE_CAPACITY)))
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
