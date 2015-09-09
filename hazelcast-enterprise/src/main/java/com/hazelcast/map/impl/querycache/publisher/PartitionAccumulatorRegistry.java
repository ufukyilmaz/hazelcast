package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.SyntheticEventFilter;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.Registry;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of mappings like {@code partitionId} to {@code Accumulator}.
 * Holds all partition accumulators of a {@link com.hazelcast.map.QueryCache QueryCache}.
 *
 * @see Accumulator
 */
public class PartitionAccumulatorRegistry implements Registry<Integer, Accumulator> {

    private final EventFilter eventFilter;
    private final AccumulatorInfo info;
    private final ConcurrentMap<Integer, Accumulator> accumulators;
    private final ConstructorFunction<Integer, Accumulator> accumulatorConstructor;
    /**
     * UUID of subscriber client/node.
     */
    private volatile String uuid;

    public PartitionAccumulatorRegistry(AccumulatorInfo info, ConstructorFunction<Integer, Accumulator> accumulatorConstructor) {
        this.info = info;
        this.eventFilter = createEventFilter();
        this.accumulatorConstructor = accumulatorConstructor;
        this.accumulators = new ConcurrentHashMap<Integer, Accumulator>();
    }

    private EventFilter createEventFilter() {
        boolean includeValue = info.isIncludeValue();
        Predicate predicate = info.getPredicate();
        EventFilter eventFilter = new QueryEventFilter(includeValue, null, predicate);
        return new SyntheticEventFilter(eventFilter);
    }

    @Override
    public Accumulator getOrCreate(Integer partitionId) {
        return ConcurrencyUtil.getOrPutIfAbsent(accumulators, partitionId, accumulatorConstructor);
    }

    @Override
    public Accumulator getOrNull(Integer partitionId) {
        return accumulators.get(partitionId);
    }

    @Override
    public Map<Integer, Accumulator> getAll() {
        return Collections.unmodifiableMap(accumulators);
    }

    @Override
    public Accumulator remove(Integer id) {
        return accumulators.remove(id);
    }

    public EventFilter getEventFilter() {
        return eventFilter;
    }

    public AccumulatorInfo getInfo() {
        return info;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}
