package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * {@link com.hazelcast.nio.serialization.DataSerializerHook} implementation for Enterprise Wan Replication
 */
public class EWRDataSerializerHook implements DataSerializerHook {

    /**
     * Id of "Enterprise Wan Replication DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ENTERPRISE_WAN_REPLICATION_DS_FACTORY, -28);

    /**
     * Id of {@link BatchWanReplicationEvent}
     */
    public static final int BATCH_WAN_REP_EVENT = 0;

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
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
    //CHECKSTYLE:ON
}
