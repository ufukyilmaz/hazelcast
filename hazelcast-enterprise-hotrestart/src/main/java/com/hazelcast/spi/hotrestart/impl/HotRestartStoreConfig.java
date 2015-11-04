package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;

import java.io.File;

import static com.hazelcast.spi.hotrestart.HotRestartStore.LOG_CATEGORY;

/**
 * Internal configuration class for the Hot Restart store.
 */
public class HotRestartStoreConfig {
    private File homeDir;
    private RamStoreRegistry ramStoreRegistry;
    private ILogger logger;
    private MemoryAllocator malloc;
    private boolean ioDisabled;
    private boolean compression;

    public HotRestartStoreConfig setHomeDir(File homeDir) {
        this.homeDir = homeDir;
        return this;
    }

    public HotRestartStoreConfig setRamStoreRegistry(RamStoreRegistry storeRegistry) {
        this.ramStoreRegistry = storeRegistry;
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

    public HotRestartStoreConfig setIoDisabled(boolean ioDisabled) {
        this.ioDisabled = ioDisabled;
        return this;
    }

    public HotRestartStoreConfig setCompression(boolean compression) {
        this.compression = compression;
        return this;
    }

    public boolean compression() {
        return compression;
    }

    public boolean ioDisabled() {
        return ioDisabled;
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
}
