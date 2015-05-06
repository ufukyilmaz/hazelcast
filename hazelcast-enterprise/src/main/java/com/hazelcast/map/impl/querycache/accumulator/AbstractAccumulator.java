package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.map.impl.querycache.event.sequence.DefaultPartitionSequencer;
import com.hazelcast.map.impl.querycache.event.sequence.PartitionSequencer;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.util.Clock;

/**
 * Contains helpers for an {@code Accumulator} implementation.
 *
 * @param <E> the type of element which will be accumulated.
 */
abstract class AbstractAccumulator<E extends Sequenced> implements Accumulator<E> {

    protected final AccumulatorInfo info;
    protected final QueryCacheContext context;
    protected final CyclicBuffer<E> buffer;
    protected final PartitionSequencer partitionSequencer;

    public AbstractAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        this.context = context;
        this.info = info;
        this.partitionSequencer = new DefaultPartitionSequencer();
        this.buffer = new DefaultCyclicBuffer<E>(info.getBufferSize());
    }

    public CyclicBuffer<E> getBuffer() {
        return buffer;
    }

    protected QueryCacheContext getContext() {
        return context;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected boolean isExpired(SingleEventData entry, long delayMillis, long now) {
        return entry != null
                && (now - entry.getCreationTime()) >= delayMillis;
    }
}
