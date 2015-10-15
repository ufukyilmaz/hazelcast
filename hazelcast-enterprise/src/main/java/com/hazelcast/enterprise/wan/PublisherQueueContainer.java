package com.hazelcast.enterprise.wan;

import com.hazelcast.instance.Node;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Queue container for wan replication publishers, provides methods to push/pull wan events to/from queues
 */
public class PublisherQueueContainer {

    private Map<Integer, PartitionWanEventContainer> publisherEventQueueMap
            = new ConcurrentHashMap<Integer, PartitionWanEventContainer>();

    public PublisherQueueContainer(Node node) {
        for (InternalPartition partition : node.getPartitionService().getPartitions()) {
            publisherEventQueueMap.put(partition.getPartitionId(), new PartitionWanEventContainer());
        }
    }

    public WanReplicationEvent pollCacheWanEvent(String nameWithPrefix, int partitionId) {
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.pollCacheWanEvent(nameWithPrefix);
    }

    public boolean publishCacheWanEvent(String nameWithPrefix, int partitionId, WanReplicationEvent replicationEvent) {
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.publishCacheWanEvent(nameWithPrefix, replicationEvent);
    }

    public WanReplicationEvent pollMapWanEvent(String mapName, int partitionId) {
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.pollMapWanEvent(mapName);
    }

    public boolean publishMapWanEvent(String mapName, int partitionId, WanReplicationEvent replicationEvent) {
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.publishMapWanEvent(mapName, replicationEvent);
    }

    public WanReplicationEvent pollRandomWanEvent(int partitionId) {
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.pollRandomWanEvent();
    }

    public Map<Integer, PartitionWanEventContainer> getPublisherEventQueueMap() {
        return publisherEventQueueMap;
    }
}
