package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.querycache.publisher.DefaultPublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheEventService;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheScheduler;
import com.hazelcast.map.impl.querycache.subscriber.NodeSubscriberContext;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Node side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class NodeQueryCacheContext implements QueryCacheContext {

    private final IFunction<String, String> listenerRegistrator =
            new IFunction<String, String>() {
                @Override
                public String apply(String mapName) {
                    return registerLocalIMapListener(mapName);
                }
            };

    private final NodeEngine nodeEngine;
    private final EnterpriseMapServiceContext mapServiceContext;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final QueryCacheScheduler queryCacheScheduler;
    private final InvokerWrapper invokerWrapper;
    // these fields are not final for testing purposes.
    private PublisherContext publisherContext;
    private SubscriberContext subscriberContext;

    public NodeQueryCacheContext(EnterpriseMapServiceContext mapServiceContext) {
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.queryCacheScheduler = new NodeQueryCacheScheduler(mapServiceContext);
        this.queryCacheEventService = new NodeQueryCacheEventService(mapServiceContext);
        this.queryCacheConfigurator = new NodeQueryCacheConfigurator(nodeEngine.getConfig(),
                nodeEngine.getConfigClassLoader(), queryCacheEventService);
        this.invokerWrapper = new NodeInvokerWrapper(nodeEngine.getOperationService());
        // init these in the end.
        this.subscriberContext = new NodeSubscriberContext(this);
        this.publisherContext = new DefaultPublisherContext(this, nodeEngine, listenerRegistrator);
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PublisherContext getPublisherContext() {
        return publisherContext;
    }

    @Override
    public SubscriberContext getSubscriberContext() {
        return subscriberContext;
    }

    @Override
    public void setSubscriberContext(SubscriberContext subscriberContext) {
        this.subscriberContext = subscriberContext;
    }

    @Override
    public QueryCacheEventService getQueryCacheEventService() {
        return queryCacheEventService;
    }

    @Override
    public QueryCacheConfigurator getQueryCacheConfigurator() {
        return queryCacheConfigurator;
    }

    @Override
    public QueryCacheScheduler getQueryCacheScheduler() {
        return queryCacheScheduler;
    }

    @Override
    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    @Override
    public Address getThisNodesAddress() {
        return nodeEngine.getThisAddress();
    }

    @Override
    public Collection<Member> getMemberList() {
        Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberImpls();
        List<Member> members = new ArrayList<Member>(memberList.size());
        members.addAll(memberList);
        return members;
    }

    @Override
    public int getPartitionId(Object object) {
        assert object != null;

        if (object instanceof Data) {
            nodeEngine.getPartitionService().getPartitionId((Data) object);
        }
        return nodeEngine.getPartitionService().getPartitionId(object);
    }

    @Override
    public InvokerWrapper getInvokerWrapper() {
        return invokerWrapper;
    }

    @Override
    public Object toObject(Object obj) {
        return mapServiceContext.toObject(obj);
    }

    private String registerLocalIMapListener(String mapName) {
        return mapServiceContext.addLocalListenerAdapter(new ListenerAdapter() {
            @Override
            public void onEvent(IMapEvent event) {
                // NOP
            }
        }, mapName);
    }
}
