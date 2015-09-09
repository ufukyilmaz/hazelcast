package com.hazelcast.map.impl;

import com.hazelcast.map.impl.event.EnterpriseMapEventPublisherImpl;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.eventservice.impl.EmptyFilter;

/**
 * Contains enterprise specific implementations of {@link MapServiceContext}
 * functionality.
 *
 * @see MapServiceContext
 */
class EnterpriseMapServiceContextImpl extends MapServiceContextImpl implements EnterpriseMapServiceContext {

    private final QueryCacheContext queryCacheContext;

    public EnterpriseMapServiceContextImpl(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.queryCacheContext = new NodeQueryCacheContext(this);
    }

    @Override
    public QueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    @Override
    public String addListenerAdapter(String cacheName, ListenerAdapter listenerAdaptor) {
        EventService eventService = getNodeEngine().getEventService();
        EventFilter filter = new EmptyFilter();
        EventRegistration registration
                = eventService.registerListener(MapService.SERVICE_NAME, cacheName, filter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addListenerAdapter(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName) {
        EventRegistration registration = getNodeEngine().getEventService().
                registerListener(MapService.SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addLocalListenerAdapter(ListenerAdapter adapter, String mapName) {
        EventService eventService = getNodeEngine().getEventService();
        EventRegistration registration = eventService.registerLocalListener(MapService.SERVICE_NAME, mapName, adapter);
        return registration.getId();
    }

    @Override
    MapEventPublisherImpl createMapEventPublisherSupport() {
        return new EnterpriseMapEventPublisherImpl(this);
    }
}
