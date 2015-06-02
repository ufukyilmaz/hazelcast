package com.hazelcast.cache.merge;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;

/**
 * A provider for {@link com.hazelcast.map.merge.MergePolicyProvider} instances.
 */
public final class CacheMergePolicyProvider {

    private final ConcurrentMap<String, CacheMergePolicy> mergePolicyMap;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, CacheMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, CacheMergePolicy>() {
        @Override
        public CacheMergePolicy createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public CacheMergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        mergePolicyMap = new ConcurrentHashMap<String, CacheMergePolicy>();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        mergePolicyMap.put(HigherHitsCacheMergePolicy.class.getName(), new HigherHitsCacheMergePolicy());
        mergePolicyMap.putIfAbsent(PassThroughCacheMergePolicy.class.getName(), new PassThroughCacheMergePolicy());
    }

    public CacheMergePolicy getMergePolicy(String className) {
        if (className == null) {
            throw new NullPointerException("Class name is mandatory!");
        }
        return ConcurrencyUtil.getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }
}
