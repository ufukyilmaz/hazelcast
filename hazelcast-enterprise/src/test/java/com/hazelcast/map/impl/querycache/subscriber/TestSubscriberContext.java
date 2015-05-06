package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.event.sequence.SubscriberSequencerProvider;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestSubscriberContext extends NodeSubscriberContext {

    private final MapSubscriberRegistry mapSubscriberRegistry;
    private final int eventCount;
    private final boolean allowEventLoss;

    public TestSubscriberContext(QueryCacheContext context, int eventCount, boolean allowEventLoss) {
        super(context);
        this.eventCount = eventCount;
        this.allowEventLoss = allowEventLoss;
        this.mapSubscriberRegistry = new TestMapSubscriberRegistry(context);
    }

    @Override
    public MapSubscriberRegistry getMapSubscriberRegistry() {
        return mapSubscriberRegistry;
    }


    private class TestMapSubscriberRegistry extends MapSubscriberRegistry {


        public TestMapSubscriberRegistry(QueryCacheContext context) {
            super(context);
        }

        @Override
        protected SubscriberRegistry createSubscriberRegistry(String mapName) {
            return new TestSubscriberRegistry(getContext(), mapName);
        }
    }


    private class TestSubscriberRegistry extends SubscriberRegistry {

        public TestSubscriberRegistry(QueryCacheContext context, String mapName) {
            super(context, mapName);
        }

        @Override
        protected SubscriberAccumulatorFactory createSubscriberAccumulatorFactory() {
            return new TestSubscriberAccumulatorFactory(getContext());
        }
    }


    private class TestSubscriberAccumulatorFactory extends SubscriberAccumulatorFactory {

        public TestSubscriberAccumulatorFactory(QueryCacheContext context) {
            super(context);
        }

        @Override
        public Accumulator createAccumulator(AccumulatorInfo info) {
            return new TestSubscriberAccumulator(getContext(), info);
        }
    }


    private class TestSubscriberAccumulator extends SubscriberAccumulator {

        public TestSubscriberAccumulator(QueryCacheContext context, AccumulatorInfo info) {
            super(context, info);
        }

        @Override
        protected SubscriberSequencerProvider createSequencerProvider() {
            return new RandomPartitionSequencer(eventCount, allowEventLoss);
        }
    }


    /**
     * Used in tests.
     */
    public class RandomPartitionSequencer implements SubscriberSequencerProvider {

        private final ConstructorFunction<Integer, Deque<Long>> sequenceConstructor
                = new ConstructorFunction<Integer, Deque<Long>>() {
            @Override
            public Deque<Long> createNew(Integer arg) {
                return createUnorderedSequence(allowEventLoss);
            }
        };

        private final ConcurrentMap<Integer, Deque<Long>> sequences = new ConcurrentHashMap<Integer, Deque<Long>>();

        private final int eventCount;
        private final boolean allowEventLoss;

        public RandomPartitionSequencer(int eventCount, boolean allowEventLoss) {
            this.eventCount = eventCount;
            this.allowEventLoss = allowEventLoss;
        }

        @Override
        public synchronized boolean compareAndSetSequence(long expect, long update, int partitionId) {
            Deque<Long> sequence = getPartitionSequence(partitionId);
            Long nexSequence = sequence.peek();
            if (nexSequence.equals(expect)) {
                sequence.remove(nexSequence);
                return sequence.add(update);
            } else {
                return false;
            }
        }

        @Override
        public synchronized long getSequence(int partitionId) {
            Deque<Long> sequence = getPartitionSequence(partitionId);
            return sequence.peek();
        }

        private Deque<Long> createUnorderedSequence(boolean allowEventLoss) {
            int lostEventCounter = 0;
            List<Long> sequence = new ArrayList<Long>();
            for (int i = 1; i <= eventCount; i++) {
                if (allowEventLoss && lostEventCounter < 1) {
                    lostEventCounter++;
                    sequence.add(i + 1L);
                } else {
                    sequence.add(Long.valueOf(i));
                }
            }
            Collections.shuffle(sequence);
            return new ArrayDeque<Long>(sequence);
        }

        private Deque<Long> getPartitionSequence(int partitionId) {
            return ConcurrencyUtil.getOrPutIfAbsent(sequences, partitionId, sequenceConstructor);
        }

        @Override
        public String toString() {
            return "RandomPartitionSequencer{"
                    + "sequences=" + sequences
                    + '}';
        }

    }
}
