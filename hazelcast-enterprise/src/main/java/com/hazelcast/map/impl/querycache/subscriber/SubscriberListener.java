package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.Collection;

/**
 * Subscriber side listener per {@code QueryCache} which listens events sent by publisher-sides.
 */
class SubscriberListener implements ListenerAdapter {

    private final AccumulatorInfo info;
    private final Accumulator accumulator;
    private final SubscriberContext subscriberContext;
    private final SerializationService serializationService;

    public SubscriberListener(QueryCacheContext context, AccumulatorInfo info) {
        this.info = info;
        this.subscriberContext = context.getSubscriberContext();
        this.accumulator = createAccumulator();
        this.serializationService = context.getSerializationService();
    }

    @Override
    public void onEvent(IMapEvent iMapEvent) {
        if (iMapEvent instanceof SingleIMapEvent) {
            QueryCacheEventData eventData = ((SingleIMapEvent) iMapEvent).getEventData();
            eventData.setSerializationService(serializationService);
            accumulator.accumulate(eventData);
            return;
        }

        if (iMapEvent instanceof BatchIMapEvent) {
            BatchIMapEvent batchIMapEvent = (BatchIMapEvent) iMapEvent;
            BatchEventData batchEventData = batchIMapEvent.getBatchEventData();
            Collection<QueryCacheEventData> events = batchEventData.getEvents();
            for (QueryCacheEventData eventData : events) {
                eventData.setSerializationService(serializationService);
                accumulator.accumulate(eventData);
            }
            return;
        }
    }

    private Accumulator createAccumulator() {
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();
        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrCreate(info.getMapName());
        return subscriberRegistry.getOrCreate(info.getCacheName());
    }
}
