package com.hazelcast.elasticmemory;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeAware;

import static java.lang.String.format;

public class InstanceStorageFactory extends StorageFactorySupport implements StorageFactory {

    private final Node node;
    private final ILogger logger;

    public InstanceStorageFactory(Node node) {
        super();
        this.node = node;
        this.logger = node.getLogger(StorageFactory.class);
    }

    public Storage createStorage() {
        String total = node.groupProperties.ELASTIC_MEMORY_TOTAL_SIZE.getValue();
        String chunk = node.groupProperties.ELASTIC_MEMORY_CHUNK_SIZE.getValue();
        boolean useUnsafe = node.groupProperties.ELASTIC_MEMORY_UNSAFE_ENABLED.getBoolean();

        if (logger.isFinestEnabled()) {
            logger.finest(format("Read %s as: %s", node.groupProperties.ELASTIC_MEMORY_TOTAL_SIZE.getName(), total));
            logger.finest(format("Read %s as: %s", node.groupProperties.ELASTIC_MEMORY_CHUNK_SIZE.getName(), chunk));
            logger.finest(format("Read %s as: %s", node.groupProperties.ELASTIC_MEMORY_UNSAFE_ENABLED.getName(), useUnsafe));
        }

        Storage storage = createStorage(total, chunk, useUnsafe, logger);
        if (storage instanceof NodeAware) {
            ((NodeAware) storage).setNode(node);
        }
        return storage;
    }
}
