package com.hazelcast.map.impl.querycache.event.sequence;

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * This class provides on-demand {@link PartitionSequencer} implementations
 * for subscriber side.
 *
 * @see PartitionSequencer
 */
public class DefaultSubscriberSequencerProvider implements SubscriberSequencerProvider {

    private static final ConstructorFunction<Integer, PartitionSequencer> PARTITION_SEQUENCER_CONSTRUCTOR
            = new ConstructorFunction<Integer, PartitionSequencer>() {
        @Override
        public PartitionSequencer createNew(Integer arg) {
            return new DefaultPartitionSequencer();
        }
    };

    private final ConcurrentMap<Integer, PartitionSequencer> partitionSequences;

    public DefaultSubscriberSequencerProvider() {
        this.partitionSequences = new ConcurrentHashMap<Integer, PartitionSequencer>();
    }

    @Override
    public boolean compareAndSetSequence(long expect, long update, int partitionId) {
        PartitionSequencer sequence = getOrCreateSequence(partitionId);
        return sequence.compareAndSetSequence(expect, update);
    }

    @Override
    public long getSequence(int partitionId) {
        PartitionSequencer sequence = getOrCreateSequence(partitionId);
        return sequence.getSequence();
    }

    @Override
    public void reset(int partitionId) {
        PartitionSequencer sequence = getOrCreateSequence(partitionId);
        sequence.reset();
    }

    private PartitionSequencer getOrCreateSequence(int partitionId) {
        return getOrPutIfAbsent(partitionSequences, partitionId, PARTITION_SEQUENCER_CONSTRUCTOR);
    }
}

