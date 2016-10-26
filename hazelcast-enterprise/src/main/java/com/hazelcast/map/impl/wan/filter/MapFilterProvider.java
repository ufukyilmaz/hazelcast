package com.hazelcast.map.impl.wan.filter;

import com.hazelcast.map.wan.filter.MapWanEventFilter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Manages {@link MapWanEventFilter} instance access/creation.
 */
public final class MapFilterProvider {
    private final ConcurrentMap<String, MapWanEventFilter> filterMap;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, MapWanEventFilter> filterConstructorFunction
            = new ConstructorFunction<String, MapWanEventFilter>() {
        @Override
        public MapWanEventFilter createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public MapFilterProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        filterMap = new ConcurrentHashMap<String, MapWanEventFilter>();
    }

    public MapWanEventFilter getFilter(String className) {
        checkNotNull(className, "Class name is mandatory!");
        return ConcurrencyUtil.getOrPutIfAbsent(filterMap, className, filterConstructorFunction);
    }
}
