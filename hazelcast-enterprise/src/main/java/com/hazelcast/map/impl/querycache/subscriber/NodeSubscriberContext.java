package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.QueryCacheContext;

/**
 * Node side implementation of {@code SubscriberContext}.
 *
 * @see SubscriberContext
 */
public class NodeSubscriberContext extends AbstractSubscriberContext {

    private final SubscriberContextSupport subscriberContextSupport;

    public NodeSubscriberContext(QueryCacheContext context) {
        super(context);
        subscriberContextSupport = new NodeSubscriberContextSupport(context.getSerializationService());
    }

    @Override
    public SubscriberContextSupport getSubscriberContextSupport() {
        return subscriberContextSupport;
    }
}
