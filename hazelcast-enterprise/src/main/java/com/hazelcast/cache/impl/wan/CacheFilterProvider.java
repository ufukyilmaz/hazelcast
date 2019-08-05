package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.wan.CacheWanEventFilter;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Manages {@link CacheWanEventFilter} instance access/creation.
 */
public final class CacheFilterProvider {
    private final ConcurrentMap<String, CacheWanEventFilter> filterMap;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, CacheWanEventFilter> filterConstructorFunction
            = new ConstructorFunction<String, CacheWanEventFilter>() {
        @Override
        public CacheWanEventFilter createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public CacheFilterProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        filterMap = new ConcurrentHashMap<String, CacheWanEventFilter>();
    }

    public CacheWanEventFilter getFilter(String className) {
        checkNotNull(className, "Class name is mandatory!");
        return ConcurrencyUtil.getOrPutIfAbsent(filterMap, className, filterConstructorFunction);
    }
}
