package com.hazelcast.map.impl;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.LocalCacheWideEventData;
import com.hazelcast.map.impl.querycache.event.LocalEntryEventData;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.nio.serialization.SerializationService;

import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.createIMapEvent;


/**
 * Includes enterprise extensions to {@link MapEventPublishingService} like {@link com.hazelcast.map.QueryCache QueryCache}
 * specific event-data dispatching functionality.
 */
class EnterpriseMapEventPublishingService extends MapEventPublishingService {

    private final Member member;
    private final SerializationService serializationService;

    protected EnterpriseMapEventPublishingService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.member = mapServiceContext.getNodeEngine().getLocalMember();
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
    }

    @Override
    public void dispatchEvent(EventData eventData, ListenerAdapter listener) {
        if (eventData instanceof SingleEventData) {
            dispatchSingleEventData((SingleEventData) eventData, listener);
            return;
        }

        if (eventData instanceof BatchEventData) {
            dispatchBatchEventData((BatchEventData) eventData, listener);
            return;
        }

        if (eventData instanceof LocalEntryEventData) {
            dispatchLocalEventData(eventData, listener);
            return;
        }

        if (eventData instanceof LocalCacheWideEventData) {
            dispatchLocalEventData(eventData, listener);
            return;
        }

        super.dispatchEvent(eventData, listener);
    }

    /**
     * Dispatches an event-data to {@link com.hazelcast.map.QueryCache QueryCache} listeners on this local
     * node.
     *
     * @param eventData {@link EventData} to be dispatched.
     * @param listener  the listener which the event will be dispatched.
     */
    private void dispatchLocalEventData(EventData eventData, ListenerAdapter listener) {
        IMapEvent event = createIMapEvent(eventData, null, member, serializationService);
        listener.onEvent(event);
    }


    private void dispatchBatchEventData(BatchEventData batchEventData, ListenerAdapter listener) {
        BatchIMapEvent mapEvent = createBatchEvent(batchEventData);
        listener.onEvent(mapEvent);
    }

    private BatchIMapEvent createBatchEvent(BatchEventData batchEventData) {
        return new BatchIMapEvent(batchEventData);
    }

    private void dispatchSingleEventData(SingleEventData eventData, ListenerAdapter listener) {
        SingleIMapEvent mapEvent = createSingleIMapEvent(eventData);
        listener.onEvent(mapEvent);
    }

    private SingleIMapEvent createSingleIMapEvent(SingleEventData eventData) {
        return new SingleIMapEvent(eventData);
    }
}
