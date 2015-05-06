package com.hazelcast.map.impl.querycache.event.sequence;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of {@link PartitionSequencer}
 *
 * @see PartitionSequencer
 */
public class DefaultPartitionSequencer implements PartitionSequencer {

    private final AtomicLong sequence;

    public DefaultPartitionSequencer() {
        this.sequence = new AtomicLong(0L);
    }

    @Override
    public long nextSequence() {
        return sequence.incrementAndGet();
    }

    @Override
    public void setSequence(long update) {
        sequence.set(update);
    }

    @Override
    public boolean compareAndSetSequence(long expect, long update) {
        return sequence.compareAndSet(expect, update);
    }

    @Override
    public long getSequence() {
        return sequence.get();
    }

    @Override
    public void reset() {
        sequence.set(0L);
    }
}
