package com.hazelcast.map.impl.querycache;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.Collection;

/**
 * A global context which contains all sub-contexts which are responsible for all
 * {@link com.hazelcast.map.QueryCache QueryCache} life-cycle management phases.
 * <p/>
 * Any functionality for a {@link com.hazelcast.map.QueryCache QueryCache} should be accessed
 * on this context. It also helps to make abstractions over auxiliary services to provide different implementations
 * on client and on node sides.
 */
public interface QueryCacheContext {

    /**
     * Returns {@link PublisherContext} for this context.
     *
     * @return {@link PublisherContext}
     * @see PublisherContext
     */
    PublisherContext getPublisherContext();

    /**
     * Returns {@link SubscriberContext} for this context.
     *
     * @return {@link SubscriberContext} for this context.
     * @see SubscriberContext
     */
    SubscriberContext getSubscriberContext();

    /**
     * Returns {@link SerializationService} for this context
     *
     * @return {@link SerializationService} for this context
     */
    SerializationService getSerializationService();

    /**
     * Returns {@link QueryCacheEventService} factory for this context
     * which will be used to create {@link QueryCacheEventService} implementations
     * for client or node sides.
     *
     * @return {@link QueryCacheEventService} for this context
     */
    QueryCacheEventService getQueryCacheEventService();

    /**
     * Returns {@link QueryCacheConfigurator} for this context
     * which will be used to create {@link QueryCacheConfigurator} implementations
     * for client or node sides.
     *
     * @return {@link QueryCacheConfigurator} for this context
     */
    QueryCacheConfigurator getQueryCacheConfigurator();

    /**
     * Returns {@link QueryCacheScheduler} for this context
     * which will be used to create {@link QueryCacheScheduler} implementations
     * for client or node sides.
     *
     * @return {@link QueryCacheScheduler} for this context
     */
    QueryCacheScheduler getQueryCacheScheduler();

    /**
     * Returns member list of the cluster. This will be used to send operations to all nodes.
     *
     * @return Member list of the cluster.
     */
    Collection<MemberImpl> getMemberList();

    /**
     * Returns {@link InvokerWrapper} for this context.
     *
     * @return {@link InvokerWrapper} for this context.
     */
    InvokerWrapper getInvokerWrapper();

    /**
     * Helper method to convert provided {@link Data}
     * to its object form.
     *
     * @param obj this object may possibly be an instance of {@link Data} type.
     * @return a new object.
     */
    Object toObject(Object obj);

    /**
     * Returns this nodes address.
     *
     * @return this nodes address.
     */
    Address getThisNodesAddress();

    /**
     * Returns partition id of the supplied object.
     *
     * @param object supplied object.
     * @return partition id
     */
    int getPartitionId(Object object);

    /**
     * Destroys everything in this context.
     */
    void destroy();

    /**
     * Only used for testing purposes.
     * <p/>
     * Sets {@link SubscriberContext} implementation used by this query-cache context.
     *
     * @param subscriberContext the supplied {@code subscriberContext} to be set.
     */
    void setSubscriberContext(SubscriberContext subscriberContext);
}
