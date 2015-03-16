package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.DataRef;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;

import java.util.logging.Level;

public class SingletonStorageFactory extends StorageFactorySupport implements StorageFactory {

    private static Storage STORAGE = null;
    private static int REF_COUNT = 0;
    private static final Object MUTEX = SingletonStorageFactory.class;
    private static final ILogger logger = Logger.getLogger(StorageFactory.class);

    public SingletonStorageFactory() {
        super();
    }

    public Storage createStorage() {
        synchronized (MUTEX) {
            if (STORAGE == null) {
                initStorage();
            }
            REF_COUNT++;
            return new StorageProxy(STORAGE);
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
            if (STORAGE != null) {
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
            logger.finest("Read " + GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE + " as: " + total);
            logger.finest("Read " + GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE + " as: " + chunk);
            logger.finest("Read " + GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED + " as: " + unsafe);
            logger.log(Level.INFO, "Initializing Singleton Storage...");
            STORAGE = createStorage(total, chunk, Boolean.parseBoolean(unsafe), logger);
        }
    }

    private static void destroyStorage() {
        synchronized (MUTEX) {
            if (STORAGE != null) {
                if (REF_COUNT <= 0) {
                    logger.log(Level.SEVERE, "Storage reference count is invalid: " + REF_COUNT);
                    REF_COUNT = 1;
                }
                REF_COUNT--;
                if (REF_COUNT == 0) {
                    logger.log(Level.INFO, "Destroying Singleton Storage ...");
                    STORAGE.destroy();
                    STORAGE = null;
                }
            } else {
                logger.log(Level.WARNING, "Storage is already destroyed !");
                if (REF_COUNT != 0) {
                    final String errorMsg = "Storage reference count must be zero (0), but it is not !!!";
                    logger.log(Level.SEVERE, errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
            }
        }
    }
}
