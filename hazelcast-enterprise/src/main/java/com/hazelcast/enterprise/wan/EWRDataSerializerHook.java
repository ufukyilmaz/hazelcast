package com.hazelcast.enterprise.wan;

import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.enterprise.wan.operation.EWRPutBackupOperation;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.enterprise.wan.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation;
import com.hazelcast.enterprise.wan.sync.WanSyncOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID;

public class EWRDataSerializerHook implements DataSerializerHook {

    /**
     * Id of "Enterprise Wan Replication DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_WAN_REPLICATION_DS_FACTORY,
            ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID);

    /**
     * Id of {@link BatchWanReplicationEvent}
     */
    public static final int BATCH_WAN_REP_EVENT = 0;

    /**
     * Id of {@link EWRPutOperation}
     */
    public static final int EWR_PUT_OPERATION = 1;

    /**
     * Id of {@link EWRPutBackupOperation}
     */
    public static final int EWR_PUT_BACKUP_OPERATION = 2;

    /**
     * Id of {@link EWRMigrationContainer}
     */
    public static final int EWR_QUEUE_CONTAINER = 3;

    /**
     * Id of {@link EWRQueueReplicationOperation}
     */
    public static final int EWR_QUEUE_REPLICATION_OPERATION = 4;

    /**
     * Id of {@link EWRRemoveBackupOperation}
     */
    public static final int EWR_REMOVE_BACKUP_OPERATION = 5;

    /**
     * Id of {@link com.hazelcast.map.impl.wan.EnterpriseMapReplicationUpdate}
     */
    public static final int MAP_REPLICATION_UPDATE = 6;

    /**
     * Id of {@link com.hazelcast.map.impl.wan.EnterpriseMapReplicationRemove}
     */
    public static final int MAP_REPLICATION_REMOVE = 7;

    /**
     * Id of {@link com.hazelcast.cache.wan.CacheReplicationUpdate}
     */
    public static final int CACHE_REPLICATION_UPDATE = 8;

    /**
     * Id of {@link com.hazelcast.cache.wan.CacheReplicationRemove}
     */
    public static final int CACHE_REPLICATION_REMOVE = 9;

    /**
     * Id of {@link com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync}
     */
    public static final int MAP_REPLICATION_SYNC = 10;

    /**
     * Id of {@link com.hazelcast.enterprise.wan.sync.WanSyncOperation}
     */
    public static final int WAN_SYNC_OPERATION = 11;

    /**
     * Id of {@link com.hazelcast.enterprise.wan.sync.GetMapPartitionDataOperation}
     */
    public static final int GET_MAP_PARTITION_DATA_OPERATION = 12;

    /**
     * Id of {@link com.hazelcast.enterprise.wan.operation.PostJoinWanOperation}
     */
    public static final int POST_JOIN_WAN_OPERATION = 13;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    //CHECKSTYLE:OFF
    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

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
                        return new WanSyncOperation();
                    case GET_MAP_PARTITION_DATA_OPERATION:
                        return new GetMapPartitionDataOperation();
                    case POST_JOIN_WAN_OPERATION:
                        return new PostJoinWanOperation();
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
    //CHECKSTYLE:ON
}
