package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

import static com.hazelcast.map.impl.querycache.ListenerRegistrationHelper.generateListenerName;

/**
 * Publish events which will be received by subscriber sides.
 *
 * @see AccumulatorProcessor
 */
public class EventPublisherAccumulatorProcessor implements AccumulatorProcessor<Sequenced> {

    private AccumulatorInfo info;
    private final QueryCacheEventService eventService;

    public EventPublisherAccumulatorProcessor(QueryCacheEventService eventService) {
        this(null, eventService);
    }

    public EventPublisherAccumulatorProcessor(AccumulatorInfo info, QueryCacheEventService eventService) {
        this.info = info;
        this.eventService = eventService;
    }

    @Override
    public void process(Sequenced sequenced) {
        String listenerName = generateListenerName(info.getMapName(), info.getCacheName());
        eventService.sendEventToSubscriber(listenerName, sequenced, sequenced.getPartitionId());
    }

    public void setInfo(AccumulatorInfo info) {
        this.info = info;
    }
}


