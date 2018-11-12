package com.hazelcast.enterprise.wan;

import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.enterprise.wan.operation.AddWanConfigBackupOperation;
import com.hazelcast.enterprise.wan.operation.AddWanConfigOperation;
import com.hazelcast.enterprise.wan.operation.AddWanConfigOperationFactory;
import com.hazelcast.enterprise.wan.operation.EWRPutBackupOperation;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.operation.MerkleTreeNodeValueComparison;
import com.hazelcast.enterprise.wan.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.operation.WanMerkleTreeNodeCompareOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventPublishOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventStarterOperation;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID;

/**
 * DataSerializerHook for Enterprise WAN objects
 */
@SuppressWarnings("checkstyle:javadocvariable")
public class EWRDataSerializerHook implements DataSerializerHook {

    /**
     * ID of "Enterprise Wan Replication DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_WAN_REPLICATION_DS_FACTORY,
            ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID);

    public static final int BATCH_WAN_REP_EVENT = 0;
    public static final int EWR_PUT_OPERATION = 1;
    public static final int EWR_PUT_BACKUP_OPERATION = 2;
    public static final int EWR_QUEUE_CONTAINER = 3;
    public static final int EWR_QUEUE_REPLICATION_OPERATION = 4;
    public static final int EWR_REMOVE_BACKUP_OPERATION = 5;
    public static final int MAP_REPLICATION_UPDATE = 6;
    public static final int MAP_REPLICATION_REMOVE = 7;
    public static final int CACHE_REPLICATION_UPDATE = 8;
    public static final int CACHE_REPLICATION_REMOVE = 9;
    public static final int MAP_REPLICATION_SYNC = 10;
    public static final int WAN_SYNC_OPERATION = 11;
    public static final int GET_MAP_PARTITION_DATA_OPERATION = 12;
    public static final int POST_JOIN_WAN_OPERATION = 13;
    public static final int WAN_OPERATION = 14;
    public static final int WAN_SYNC_EVENT = 15;
    public static final int WAN_ANTI_ENTROPY_RESULT = 16;
    public static final int WAN_ANTI_ENTROPY_EVENT_STARTER_OPERATION = 17;
    public static final int WAN_CONSISTENCY_CHECK_EVENT = 18;
    public static final int WAN_MERKLE_TREE_NODE_COMPARE_OPERATION = 19;
    public static final int MERKLE_TREE_NODE_VALUE_COMPARISON = 20;
    public static final int MAP_REPLICATION_MERKLE_TREE_NODE = 21;
    public static final int ADD_WAN_CONFIG_OPERATION_FACTORY = 22;
    public static final int ADD_WAN_CONFIG_OPERATION = 23;
    public static final int ADD_WAN_CONFIG_BACKUP_OPERATION = 24;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case BATCH_WAN_REP_EVENT:
                        return new BatchWanReplicationEvent();
                    case EWR_PUT_OPERATION:
                        return new EWRPutOperation();
                    case EWR_PUT_BACKUP_OPERATION:
                        return new EWRPutBackupOperation();
                    case EWR_QUEUE_CONTAINER:
                        return new EWRMigrationContainer();
                    case EWR_QUEUE_REPLICATION_OPERATION:
                        return new EWRQueueReplicationOperation();
                    case EWR_REMOVE_BACKUP_OPERATION:
                        return new EWRRemoveBackupOperation();
                    case MAP_REPLICATION_UPDATE:
                        return new EnterpriseMapReplicationUpdate();
                    case MAP_REPLICATION_REMOVE:
                        return new EnterpriseMapReplicationRemove();
                    case CACHE_REPLICATION_UPDATE:
                        return new CacheReplicationUpdate();
                    case CACHE_REPLICATION_REMOVE:
                        return new CacheReplicationRemove();
                    case MAP_REPLICATION_SYNC:
                        return new EnterpriseMapReplicationSync();
                    case WAN_SYNC_OPERATION:
                        return new WanAntiEntropyEventPublishOperation();
                    case GET_MAP_PARTITION_DATA_OPERATION:
                        return new GetMapPartitionDataOperation();
                    case POST_JOIN_WAN_OPERATION:
                        return new PostJoinWanOperation();
                    case WAN_OPERATION:
                        return new WanOperation();
                    case WAN_SYNC_EVENT:
                        return new WanSyncEvent();
                    case WAN_ANTI_ENTROPY_RESULT:
                        return new WanAntiEntropyEventResult();
                    case WAN_ANTI_ENTROPY_EVENT_STARTER_OPERATION:
                        return new WanAntiEntropyEventStarterOperation();
                    case WAN_CONSISTENCY_CHECK_EVENT:
                        return new WanConsistencyCheckEvent();
                    case WAN_MERKLE_TREE_NODE_COMPARE_OPERATION:
                        return new WanMerkleTreeNodeCompareOperation();
                    case MERKLE_TREE_NODE_VALUE_COMPARISON:
                        return new MerkleTreeNodeValueComparison();
                    case MAP_REPLICATION_MERKLE_TREE_NODE:
                        return new EnterpriseMapReplicationMerkleTreeNode();
                    case ADD_WAN_CONFIG_OPERATION_FACTORY:
                        return new AddWanConfigOperationFactory();
                    case ADD_WAN_CONFIG_OPERATION:
                        return new AddWanConfigOperation();
                    case ADD_WAN_CONFIG_BACKUP_OPERATION:
                        return new AddWanConfigBackupOperation();
                    default:
                        throw new IllegalArgumentException("Unknown type ID: " + typeId);
                }
            }
        };
    }
}
