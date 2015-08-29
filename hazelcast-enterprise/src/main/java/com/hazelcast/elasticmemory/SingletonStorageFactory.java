package com.hazelcast.elasticmemory;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.internal.storage.DataRef;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;

import java.util.logging.Level;

public class SingletonStorageFactory extends StorageFactorySupport implements StorageFactory {

    private static final ILogger LOGGER = Logger.getLogger(StorageFactory.class);
    private static final Object MUTEX = SingletonStorageFactory.class;

    private static Storage storage;
    private static int refCount;

    public SingletonStorageFactory() {
        super();
    }

    public Storage createStorage() {
        synchronized (MUTEX) {
            if (storage == null) {
                initStorage();
            }
            refCount++;
            return new StorageProxy(storage);
        }
    }

    private class StorageProxy implements Storage {
        Storage storage;

        StorageProxy(Storage storage) {
            super();
            this.storage = storage;
        }

        public DataRef put(int hash, Data data) {
            return storage.put(hash, data);
        }

        public Data get(int hash, DataRef entry) {
            return storage.get(hash, entry);
        }

        public void remove(int hash, DataRef entry) {
            storage.remove(hash, entry);
        }

        public void destroy() {
            synchronized (MUTEX) {
                if (storage != null) {
                    storage = null;
                    destroyStorage();
                }
            }
        }
    }

    private static void initStorage() {
        synchronized (MUTEX) {
            if (storage != null) {
                throw new IllegalStateException("Storage is already initialized!");
            }
            final String total = System.getProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE);
            final String chunk = System.getProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE);
            if (total == null || chunk == null) {
                throw new IllegalArgumentException("Both '" + GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE
                        + "' and '" + GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE
                        + "' system properties are mandatory!");
            }

            final String unsafe = System.getProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED);
            LOGGER.finest("Read " + GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE + " as: " + total);
            LOGGER.finest("Read " + GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE + " as: " + chunk);
            LOGGER.finest("Read " + GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED + " as: " + unsafe);
            LOGGER.log(Level.INFO, "Initializing Singleton Storage...");
            storage = createStorage(total, chunk, Boolean.parseBoolean(unsafe), LOGGER);
        }
    }

    private static void destroyStorage() {
        synchronized (MUTEX) {
            if (storage != null) {
                if (refCount <= 0) {
                    LOGGER.log(Level.SEVERE, "Storage reference count is invalid: " + refCount);
                    refCount = 1;
                }
                refCount--;
                if (refCount == 0) {
                    LOGGER.log(Level.INFO, "Destroying Singleton Storage ...");
                    storage.destroy();
                    storage = null;
                }
            } else {
                LOGGER.log(Level.WARNING, "Storage is already destroyed !");
                if (refCount != 0) {
                    final String errorMsg = "Storage reference count must be zero (0), but it is not !!!";
                    LOGGER.log(Level.SEVERE, errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
            }
        }
    }
}
