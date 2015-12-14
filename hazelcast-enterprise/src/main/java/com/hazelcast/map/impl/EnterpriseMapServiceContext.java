package com.hazelcast.map.impl;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.wan.filter.MapFilterProvider;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.hotrestart.HotRestartStore;

/**
 * Enterprise version of {@link com.hazelcast.map.impl.MapServiceContext}
 */
public interface EnterpriseMapServiceContext extends MapServiceContext {

    QueryCacheContext getQueryCacheContext();

    String addListenerAdapter(String cacheName, ListenerAdapter listenerAdaptor);

    String addListenerAdapter(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName);

    String addLocalListenerAdapter(ListenerAdapter listenerAdaptor, String mapName);

    MapFilterProvider getMapFilterProvider();

    HotRestartStore getOnHeapHotRestartStoreForCurrentThread();

    HotRestartStore getOffHeapHotRestartStoreForCurrentThread();
}
