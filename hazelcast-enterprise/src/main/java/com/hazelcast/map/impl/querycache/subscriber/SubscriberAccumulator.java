package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.sequence.DefaultSubscriberSequencerProvider;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.map.impl.querycache.event.sequence.SubscriberSequencerProvider;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.publishEventLost;


/**
 * If all incoming events are in correct sequence order, this accumulator applies those events to
 * {@link com.hazelcast.map.QueryCache QueryCache}.
 * Otherwise, it informs registered callback if there is any.
 * <p/>
 * This class can be accessed by multiple-threads at a time.
 */
public class SubscriberAccumulator extends BasicAccumulator<QueryCacheEventData> {

    private final ILogger logger = Logger.getLogger(getClass());
    private final AccumulatorHandler handler;
    private final SubscriberSequencerProvider sequenceProvider;
    /**
     * When a partitions sequence order is broken, it will be registered here.
     */
    private final ConcurrentMap<Integer, Long> brokenSequences;

    public SubscriberAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
        this.handler = createAccumulatorHandler(context);
        this.sequenceProvider = createSequencerProvider();
        this.brokenSequences = new ConcurrentHashMap<Integer, Long>();
    }

    @Override
    public void accumulate(QueryCacheEventData event) {
        if (!isApplicable(event)) {
            return;
        }
        addQueryCache(event);
    }

    /**
     * Checks whether the event data is applicable to the query cache.
     */
    private boolean isApplicable(QueryCacheEventData event) {
        return getInfo().isPublishable()
                && hasNextSequence(event);
    }

    private boolean hasNextSequence(Sequenced event) {
        int partitionId = event.getPartitionId();
        long newSequence = event.getSequence();

        // if sequence is -1, it means an instance sequence should be reset.
        if (newSequence == -1L) {
            sequenceProvider.reset(partitionId);
            return false;
        }
        long currentSequence = sequenceProvider.getSequence(partitionId);

        // if new-sequence is not the next-expected-sequence.
        long expectedSequence = currentSequence + 1L;
        if (newSequence != expectedSequence) {
            logger.warning("Event lost detected for partitionId = " + partitionId
                    + ", expected sequence = " + expectedSequence
                    + " but found = " + newSequence);
            Long prev = brokenSequences.putIfAbsent(partitionId, expectedSequence);
            if (prev == null) {
                publishEventLost(context, info.getMapName(), info.getCacheName(), partitionId);
            }
            return false;
        }
        return sequenceProvider.compareAndSetSequence(currentSequence, newSequence, partitionId);
    }

    private SubscriberAccumulatorHandler createAccumulatorHandler(QueryCacheContext context) {
        AccumulatorInfo info = getInfo();
        boolean includeValue = info.isIncludeValue();
        String cacheName = info.getCacheName();
        SubscriberContext subscriberContext = context.getSubscriberContext();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();
        InternalQueryCache queryCache = queryCacheFactory.getOrNull(cacheName);
        SerializationService serializationService = context.getSerializationService();
        return new SubscriberAccumulatorHandler(includeValue, queryCache, serializationService);
    }

    private void addQueryCache(QueryCacheEventData eventData) {
        handler.handle(eventData, false);
    }

    protected SubscriberSequencerProvider createSequencerProvider() {
        return new DefaultSubscriberSequencerProvider();
    }

    public ConcurrentMap<Integer, Long> getBrokenSequences() {
        return brokenSequences;
    }
}

