package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.HotRestartStore.LOG_CATEGORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Internal configuration class for the Hot Restart store.
 */
public class HotRestartStoreConfig {
    private String storeName;
    private File homeDir;
    private RamStoreRegistry ramStoreRegistry;
    private ILogger logger;
    private MetricsRegistry metricsRegistry;
    private MemoryAllocator malloc;

    public HotRestartStoreConfig setStoreName(String storeName) {
        this.storeName = storeName;
        return this;
    }

    public HotRestartStoreConfig setHomeDir(File homeDir) {
        this.homeDir = homeDir;
        return this;
    }

    public HotRestartStoreConfig setRamStoreRegistry(RamStoreRegistry storeRegistry) {
        this.ramStoreRegistry = storeRegistry;
        return this;
    }

    public HotRestartStoreConfig setMetricsRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        return this;
    }

    public HotRestartStoreConfig setLoggingService(LoggingService loggingService) {
        this.logger = loggingService.getLogger(LOG_CATEGORY);
        return this;
    }

    public HotRestartStoreConfig setMalloc(MemoryAllocator malloc) {
        this.malloc = malloc;
        return this;
    }

    public HotRestartStoreConfig validateAndCreateHomeDir() {
        checkNotNull(homeDir, "homeDir is null");
        try {
            final File canonicalHome = homeDir.getCanonicalFile();
            if (canonicalHome.exists() && !canonicalHome.isDirectory()) {
                throw new HotRestartException("Path refers to a non-directory: " + canonicalHome);
            }
            if (!canonicalHome.exists() && !canonicalHome.mkdirs()) {
                throw new HotRestartException("Could not create the base directory " + canonicalHome);
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
        return this;
    }

    public String storeName() {
        return storeName;
    }

    public File homeDir() {
        return homeDir;
    }

    public ILogger logger() {
        return logger;
    }

    public MemoryAllocator malloc() {
        return malloc;
    }

    public RamStoreRegistry ramStoreRegistry() {
        return ramStoreRegistry;
    }

    public MetricsRegistry metricsRegistry() {
        return metricsRegistry;
    }

}
