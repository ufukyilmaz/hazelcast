package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.Address;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Handler for processing of event data in an {@link Accumulator Accumulator}.
 * Processing is done by the help of {@link AccumulatorProcessor}
 *
 * @see EventPublisherAccumulatorProcessor
 */
public class PublisherAccumulatorHandler implements AccumulatorHandler<Sequenced> {

    private final QueryCacheContext context;
    private final AccumulatorProcessor<Sequenced> processor;
    private Queue<SingleEventData> eventCollection;

    public PublisherAccumulatorHandler(QueryCacheContext context, AccumulatorProcessor<Sequenced> processor) {
        this.context = context;
        this.processor = processor;
    }

    @Override
    public void handle(Sequenced eventData, boolean lastElement) {
        if (eventCollection == null) {
            eventCollection = new ArrayDeque<SingleEventData>();
        }

        eventCollection.add((SingleEventData) eventData);

        if (lastElement) {
            process();
        }
    }

    private void process() {
        Queue<SingleEventData> eventCollection = this.eventCollection;
        if (eventCollection.isEmpty()) {
            return;
        }

        if (eventCollection.size() < 2) {
            SingleEventData eventData = eventCollection.poll();
            processor.process(eventData);
        } else {
            sendInBatches(eventCollection);
        }
    }

    private void sendInBatches(Queue<SingleEventData> events) {
        Map<Integer, Collection<SingleEventData>> partitionToEventDataMap = createPartitionToEventDataMap(events);
        sendToSubscriber(partitionToEventDataMap);
    }

    private Map<Integer, Collection<SingleEventData>> createPartitionToEventDataMap(Queue<SingleEventData> events) {
        if (events.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Integer, Collection<SingleEventData>> map = new HashMap<Integer, Collection<SingleEventData>>();

        do {
            SingleEventData eventData = events.poll();
            if (eventData == null) {
                break;
            }
            int partitionId = eventData.getPartitionId();

            Collection<SingleEventData> eventDataList = map.get(partitionId);
            if (eventDataList == null) {
                eventDataList = new ArrayList<SingleEventData>();
                map.put(partitionId, eventDataList);
            }
            eventDataList.add(eventData);

        } while (true);

        return map;
    }

    private void sendToSubscriber(Map<Integer, Collection<SingleEventData>> map) {
        Set<Map.Entry<Integer, Collection<SingleEventData>>> entries = map.entrySet();
        for (Map.Entry<Integer, Collection<SingleEventData>> entry : entries) {
            Integer partitionId = entry.getKey();
            Collection<SingleEventData> eventData = entry.getValue();
            String thisNodesAddress = getThisNodesAddress();
            BatchEventData batchEventData = new BatchEventData(eventData, thisNodesAddress, partitionId);
            processor.process(batchEventData);
        }
    }


    private String getThisNodesAddress() {
        Address thisAddress = context.getThisNodesAddress();
        return thisAddress.toString();
    }
}
