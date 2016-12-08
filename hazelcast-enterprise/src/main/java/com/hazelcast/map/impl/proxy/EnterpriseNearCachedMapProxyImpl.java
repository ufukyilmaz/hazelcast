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
 * A server-side {@code IEnterpriseMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class EnterpriseNearCachedMapProxyImpl<K, V> extends NearCachedMapProxyImpl<K, V> implements IEnterpriseMap<K, V> {

    public EnterpriseNearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
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
