package com.hazelcast.enterprise.wan;

import com.hazelcast.enterprise.wan.operation.EWRPutBackupOperation;
import com.hazelcast.enterprise.wan.operation.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.operation.EWRPutOperation;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.enterprise.wan.operation.EWRRemoveBackupOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID;

/**
 * {@link com.hazelcast.internal.serialization.DataSerializerHook} implementation for Enterprise Wan Replication
 */
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
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
    //CHECKSTYLE:ON
}
