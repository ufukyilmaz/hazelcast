package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;
import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.serialization.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link com.hazelcast.map.impl.querycache.accumulator.Accumulator Accumulator} which coalesces
 * keys during accumulation.
 */
public class CoalescingPublisherAccumulator extends BasicAccumulator<QueryCacheEventData> {

    /**
     * Index map to hold last unpublished event sequence per key.
     */
    private final Map<Data, Long> index;

    public CoalescingPublisherAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
        index = new HashMap<Data, Long>();
    }

    @Override
    public void accumulate(QueryCacheEventData eventData) {
        setSequence(eventData);
        getBuffer().add(eventData);

        AccumulatorInfo info = getInfo();
        if (!info.isPublishable()) {
            return;
        }

        poll(handler, info.getBatchSize());
        poll(handler, info.getDelaySeconds(), TimeUnit.SECONDS);
    }

    private void setSequence(QueryCacheEventData eventData) {
        Data dataKey = eventData.getDataKey();
        Long sequence = index.get(dataKey);
        if (sequence != null) {
            eventData.setSequence(sequence);
        } else {
            long nextSequence = partitionSequencer.nextSequence();
            eventData.setSequence(nextSequence);
            index.put(dataKey, nextSequence);
        }
    }

    @Override
    protected AccumulatorProcessor createAccumulatorProcessor(AccumulatorInfo info, QueryCacheEventService eventService) {
        return new CoalescedEventAccumulatorProcessor(info, eventService);
    }


    /**
     * {@link EventPublisherAccumulatorProcessor} which additionally clears {@link #index} upon publishing.
     */
    private class CoalescedEventAccumulatorProcessor extends EventPublisherAccumulatorProcessor {

        public CoalescedEventAccumulatorProcessor(AccumulatorInfo info, QueryCacheEventService eventService) {
            super(info, eventService);
        }

        @Override
        public void process(Sequenced sequenced) {
            super.process(sequenced);

            QueryCacheEventData eventData = (QueryCacheEventData) sequenced;
            Data dataKey = eventData.getDataKey();
            index.remove(dataKey);
        }
    }
}
