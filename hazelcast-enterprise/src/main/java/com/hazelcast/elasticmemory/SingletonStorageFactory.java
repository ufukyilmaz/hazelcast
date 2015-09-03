package com.hazelcast.elasticmemory;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.internal.storage.DataRef;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;

import static java.lang.String.format;

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

        @SuppressWarnings("unchecked")
        public Data get(int hash, DataRef entry) {
            return storage.get(hash, entry);
        }

        @SuppressWarnings("unchecked")
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

            String total = System.getProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE);
            String chunk = System.getProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE);
            String unsafe = System.getProperty(GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED);

            if (total == null || chunk == null) {
                throw new IllegalArgumentException(format("Both '%s' and '%s' system properties are mandatory!",
                        GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE));
            }

            if (LOGGER.isFinestEnabled()) {
                LOGGER.finest(format("Read %s as: %s", GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, total));
                LOGGER.finest(format("Read %s as: %s", GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, chunk));
                LOGGER.finest(format("Read %s as: %s", GroupProperties.PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, unsafe));
            }

            LOGGER.info("Initializing Singleton Storage...");
            storage = createStorage(total, chunk, Boolean.parseBoolean(unsafe), LOGGER);
        }
    }

    private static void destroyStorage() {
        synchronized (MUTEX) {
            if (storage != null) {
                if (refCount <= 0) {
                    LOGGER.severe("Storage reference count is invalid: " + refCount);
                    refCount = 1;
                }
                refCount--;
                if (refCount == 0) {
                    LOGGER.info("Destroying Singleton Storage ...");
                    storage.destroy();
                    storage = null;
                }
            } else {
                LOGGER.warning("Storage is already destroyed !");
                if (refCount != 0) {
                    String errorMsg = "Storage reference count must be zero (0), but it is not !!!";
                    LOGGER.severe(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
            }
        }
    }
}
