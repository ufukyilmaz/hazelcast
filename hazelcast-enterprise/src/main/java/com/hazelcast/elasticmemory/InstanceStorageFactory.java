package com.hazelcast.elasticmemory;


import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeAware;
import com.hazelcast.internal.storage.Storage;

import java.util.logging.Level;

public class InstanceStorageFactory extends StorageFactorySupport implements StorageFactory {

    final Node node;
    final ILogger logger;

    public InstanceStorageFactory(Node node) {
        super();
        this.node = node;
        logger = node.getLogger(StorageFactory.class);
    }

    public Storage createStorage() {
        String total = node.groupProperties.ELASTIC_MEMORY_TOTAL_SIZE.getValue();
        logger.log(Level.FINEST, "Read " + node.groupProperties.ELASTIC_MEMORY_TOTAL_SIZE.getName() + " as: " + total);
        String chunk = node.groupProperties.ELASTIC_MEMORY_CHUNK_SIZE.getValue();
        logger.log(Level.FINEST, "Read " + node.groupProperties.ELASTIC_MEMORY_CHUNK_SIZE.getName() + " as: " + chunk);
        boolean useUnsafe = node.groupProperties.ELASTIC_MEMORY_UNSAFE_ENABLED.getBoolean();
        logger.log(Level.FINEST, "Read " + node.groupProperties.ELASTIC_MEMORY_UNSAFE_ENABLED.getName() + " as: " + useUnsafe);
        Storage storage = createStorage(total, chunk, useUnsafe, logger);
        if (storage instanceof NodeAware) {
            ((NodeAware) storage).setNode(node);
        }
        return storage;
    }

}
