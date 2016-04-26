package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.LoggingService;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.spi.hotrestart.impl.testsupport.MockStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.testsupport.TestProfile;
import com.hazelcast.util.collection.Long2LongHashMap;

import java.io.File;
import java.util.Map;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_PAGE_SIZE;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createStoreRegistry;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.exercise;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.fillStore;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.logger;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.metricsRegistry;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.summarize;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.verifyRestartedStore;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HotRestartStoreExerciser {

    private final HotRestartStoreConfig cfg;
    private final MemoryAllocator malloc;
    private final TestProfile profile;

    HotRestartStoreExerciser(File testingHome, TestProfile profile) {
        this.profile = profile;
        final LoggingService loggingService = createLoggingService();
        logger = loggingService.getLogger("hotrestart-test");
        final HotRestartStoreConfig cfg = new HotRestartStoreConfig()
                .setHomeDir(new File(testingHome, "hr-store"))
                .setLoggingService(loggingService)
                .setMetricsRegistry(metricsRegistry(loggingService));
        final int offHeapMb = profile.offHeapMb;
        if (offHeapMb > 0) {
            final PoolingMemoryManager malloc = new PoolingMemoryManager(
                    new MemorySize(offHeapMb, MEGABYTES), DEFAULT_MIN_BLOCK_SIZE, DEFAULT_PAGE_SIZE,
                    profile.offHeapMetadataPercentage);
            malloc.registerThread(currentThread());
            this.malloc = malloc;
            cfg.setMalloc(malloc.getSystemAllocator());
        } else {
            this.malloc = null;
        }
        this.cfg = cfg;
    }

    void proceed() throws Exception {
        profile.build();
        final int restartCount = profile.restartCount;
        if (restartCount > 0) {
            testRestart(restartCount);
        } else {
            testFullOperation();
        }
    }

    private void testRestart(int restartCount) throws InterruptedException {
        for (int i = 0; i < restartCount; i++) {
            final MockStoreRegistry reg = newStoreRegistry();
            SECONDS.sleep(5);
            reg.closeHotRestartStore();
        }
    }

    private void testFullOperation() throws Exception {
        delete(cfg.homeDir());
        MockStoreRegistry reg = newStoreRegistry();
        try {
            if (reg.isEmpty()) {
                logger.info("Store empty, filling");
                Thread.sleep(200);
                fillStore(reg, profile) ;
            }
            for (int i = 0; i < profile.testCycleCount; i++) {
                exercise(reg, cfg, profile);
                final Map<Long, Long2LongHashMap> summary = summarize(reg);
                reg.closeHotRestartStore();
                reg.disposeRecordStores();
                logger.info("\n\nRestart\n");
                reg = newStoreRegistry();
                verifyRestartedStore(summary, reg);
            }
            reg.closeHotRestartStore();
            reg.disposeRecordStores();
        } finally {
        }
    }

    private MockStoreRegistry newStoreRegistry() throws InterruptedException {
        return createStoreRegistry(cfg, malloc);
    }
}




