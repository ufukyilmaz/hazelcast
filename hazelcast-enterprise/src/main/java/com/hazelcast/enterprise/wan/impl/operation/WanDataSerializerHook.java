package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.cache.impl.wan.WanEnterpriseCacheAddOrUpdateEvent;
import com.hazelcast.cache.impl.wan.WanEnterpriseCacheRemoveEvent;
import com.hazelcast.enterprise.wan.impl.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.impl.WanEventMigrationContainer;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.enterprise.wan.impl.replication.WanEventBatch;
import com.hazelcast.enterprise.wan.impl.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEventPublishOperation;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEventStarterOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.wan.WanEnterpriseMapAddOrUpdateEvent;
import com.hazelcast.map.impl.wan.WanEnterpriseMapMerkleTreeNode;
import com.hazelcast.map.impl.wan.WanEnterpriseMapRemoveEvent;
import com.hazelcast.map.impl.wan.WanEnterpriseMapSyncEvent;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID;

/**
 * DataSerializerHook for Enterprise WAN objects
 */
@SuppressWarnings("checkstyle:javadocvariable")
public class WanDataSerializerHook implements DataSerializerHook {

    /**
     * ID of "Enterprise Wan Replication DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_WAN_REPLICATION_DS_FACTORY,
            ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID);

    public static final int BATCH_WAN_REP_EVENT = 0;
    public static final int EWR_PUT_OPERATION = 1;
    public static final int EWR_PUT_BACKUP_OPERATION = 2;
    public static final int WAN_EVENT_MIGRATION_CONTAINER = 3;
    public static final int MAP_REPLICATION_UPDATE = 4;
    public static final int MAP_REPLICATION_REMOVE = 5;
    public static final int CACHE_REPLICATION_UPDATE = 6;
    public static final int CACHE_REPLICATION_REMOVE = 7;
    public static final int MAP_REPLICATION_SYNC = 8;
    public static final int WAN_SYNC_OPERATION = 9;
    public static final int GET_MAP_PARTITION_DATA_OPERATION = 10;
    public static final int POST_JOIN_WAN_OPERATION = 11;
    public static final int WAN_OPERATION = 12;
    public static final int WAN_SYNC_EVENT = 13;
    public static final int WAN_ANTI_ENTROPY_RESULT = 14;
    public static final int WAN_ANTI_ENTROPY_EVENT_STARTER_OPERATION = 15;
    public static final int WAN_CONSISTENCY_CHECK_EVENT = 16;
    public static final int WAN_MERKLE_TREE_NODE_COMPARE_OPERATION = 17;
    public static final int MERKLE_TREE_NODE_VALUE_COMPARISON = 18;
    public static final int MAP_REPLICATION_MERKLE_TREE_NODE = 19;
    public static final int ADD_WAN_CONFIG_OPERATION_FACTORY = 20;
    public static final int ADD_WAN_CONFIG_OPERATION = 21;
    public static final int ADD_WAN_CONFIG_BACKUP_OPERATION = 22;
    public static final int REMOVE_WAN_EVENT_BACKUPS_OPERATION = 23;
    public static final int WAN_PROTOCOL_NEGOTIATION_OPERATION = 24;
    public static final int WAN_PROTOCOL_NEGOTIATION_RESPONSE = 25;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case BATCH_WAN_REP_EVENT:
                    return new WanEventBatch();
                case EWR_PUT_OPERATION:
                    return new WanPutOperation();
                case EWR_PUT_BACKUP_OPERATION:
                    return new WanPutBackupOperation();
                case WAN_EVENT_MIGRATION_CONTAINER:
                    return new WanEventMigrationContainer();
                case MAP_REPLICATION_UPDATE:
                    return new WanEnterpriseMapAddOrUpdateEvent();
                case MAP_REPLICATION_REMOVE:
                    return new WanEnterpriseMapRemoveEvent();
                case CACHE_REPLICATION_UPDATE:
                    return new WanEnterpriseCacheAddOrUpdateEvent();
                case CACHE_REPLICATION_REMOVE:
                    return new WanEnterpriseCacheRemoveEvent();
                case MAP_REPLICATION_SYNC:
                    return new WanEnterpriseMapSyncEvent();
                case WAN_SYNC_OPERATION:
                    return new WanAntiEntropyEventPublishOperation();
                case GET_MAP_PARTITION_DATA_OPERATION:
                    return new GetMapPartitionDataOperation();
                case POST_JOIN_WAN_OPERATION:
                    return new PostJoinWanOperation();
                case WAN_OPERATION:
                    return new WanEventContainerOperation();
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
                    return new WanEnterpriseMapMerkleTreeNode();
                case ADD_WAN_CONFIG_OPERATION_FACTORY:
                    return new AddWanConfigOperationFactory();
                case ADD_WAN_CONFIG_OPERATION:
                    return new AddWanConfigOperation();
                case ADD_WAN_CONFIG_BACKUP_OPERATION:
                    return new AddWanConfigBackupOperation();
                case REMOVE_WAN_EVENT_BACKUPS_OPERATION:
                    return new RemoveWanEventBackupsOperation();
                case WAN_PROTOCOL_NEGOTIATION_OPERATION:
                    return new WanProtocolNegotiationOperation();
                case WAN_PROTOCOL_NEGOTIATION_RESPONSE:
                    return new WanProtocolNegotiationResponse();
                default:
                    throw new IllegalArgumentException("Unknown type ID: " + typeId);
            }
        };
    }
}
