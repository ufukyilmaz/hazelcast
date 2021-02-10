package com.hazelcast.jet.impl.aggregate;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_AGGREGATE_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_AGGREGATE_DS_FACTORY_ID;

public class AggregateDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link AggregateOpAggregator} class.
     */
    public static final int AGGREGATE_OP_AGGREGATOR = 0;

    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_AGGREGATE_DS_FACTORY,
            JET_AGGREGATE_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case AGGREGATE_OP_AGGREGATOR:
                    return new AggregateOpAggregator<>();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        };
    }
}
