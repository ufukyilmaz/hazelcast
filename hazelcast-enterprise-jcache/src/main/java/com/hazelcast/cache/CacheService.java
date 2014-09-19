package com.hazelcast.cache;

import com.hazelcast.cache.client.CacheInvalidationListener;
import com.hazelcast.cache.client.CacheInvalidationMessage;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 05/02/14
 */
public class CacheService implements ManagedService, RemoteService,
        MigrationAwareService, EventPublishingService<CacheInvalidationMessage, CacheInvalidationListener> {

    public final static String SERVICE_NAME = "hz:impl:cacheService";
    private ILogger logger;
    private NodeEngine nodeEngine;
    private CachePartitionSegment[] segments;
    private final ConcurrentMap<String, CacheStatsImpl> statsMap = new ConcurrentHashMap<String, CacheStatsImpl>(1000);
    private final ConstructorFunction<String, CacheStatsImpl> cacheStatsConstructorFunction = new ConstructorFunction<String, CacheStatsImpl>() {
        public CacheStatsImpl createNew(String key) {
            return new CacheStatsImpl();
        }
    };


    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = new CachePartitionSegment(nodeEngine, this, i);
        }
    }

    @Override
    public void reset() {
        shutdown(false);
    }

    @Override
    public void shutdown(boolean terminate) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheSegmentDestroyOperation> ops = new ArrayList<CacheSegmentDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasAnyCache()) {
                CacheSegmentDestroyOperation op = new CacheSegmentDestroyOperation();
                ops.add(op);
                op.setPartitionId(segment.getPartitionId()).setNodeEngine(nodeEngine).setService(this);
                operationService.executeOperation(op);
            }
        }
        for (CacheSegmentDestroyOperation op : ops) {
            try {
                op.awaitCompletion(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    CacheRecordStore getOrCreateCache(String name, int partitionId) {
        return segments[partitionId].getOrCreateCache(name);
    }

    CacheRecordStore getCache(String name, int partitionId) {
        return segments[partitionId].getCache(name);
    }

    CachePartitionSegment getSegment(int partitionId) {
        return segments[partitionId];
    }

    public CacheStatsImpl getOrCreateCacheStats(String name){
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, cacheStatsConstructorFunction);
    }

    /**
     * Does forced eviction on one or more caches... Runs on the operation threads..
     */
    int forceEvict(String name, int originalPartitionId) {
        int evicted = 0;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = nodeEngine.getOperationService().getPartitionOperationThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                CacheRecordStore cache = getCache(name, partitionId);
                if (cache != null) {
                    evicted += cache.forceEvict();
                }
            }
        }
        return evicted;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        segments[partitionId].clear();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        EnterpriseSerializationService serializationService = getSerializationService();
        if (serializationService.getMemoryManager() == null) {
            throw new IllegalStateException("OffHeap memory should be enabled and configured " +
                    "to be able to use ICache!");
        }
        return new CacheProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheDestroyOperation> ops = new ArrayList<CacheDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasCache(objectName)) {
                CacheDestroyOperation op = new CacheDestroyOperation(objectName);
                ops.add(op);
                op.setPartitionId(segment.getPartitionId()).setNodeEngine(nodeEngine).setService(this);
                operationService.executeOperation(op);
            }
        }
        for (CacheDestroyOperation op : ops) {
            try {
                op.awaitCompletion(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Object toObject(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    public String addInvalidationListener(String name, CacheInvalidationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, name, listener);
        return registration.getId();
    }

    public void dispatchEvent(CacheInvalidationMessage event, CacheInvalidationListener listener) {
        listener.send(event);
    }

    public void sendInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (!registrations.isEmpty()) {
            EnterpriseSerializationService ss = getSerializationService();
            Data event = ss.convertData(key, DataType.HEAP);
            eventService.publishEvent(SERVICE_NAME, registrations,
                    new CacheInvalidationMessage(name, event, sourceUuid), name.hashCode());
        }
    }

    EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) nodeEngine.getSerializationService();
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public String toString() {
        return "CacheService[" + SERVICE_NAME + "]";
    }

}
