package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.spi.NodeEngine;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Proxy implementation of {@link com.hazelcast.core.IMap} interface
 * which includes enterprise version specific extensions.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 */
public class EnterpriseMapProxyImpl<K, V> extends MapProxyImpl<K, V> implements IEnterpriseMap<K, V> {

    public EnterpriseMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {
        checkTrue(NATIVE != mapConfig.getInMemoryFormat(), "NATIVE storage format is not supported for MapReduce");
        return super.aggregate(supplier, aggregation);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation, JobTracker jobTracker) {
        checkTrue(NATIVE != mapConfig.getInMemoryFormat(), "NATIVE storage format is not supported for MapReduce");
        return super.aggregate(supplier, aggregation, jobTracker);
    }
}
