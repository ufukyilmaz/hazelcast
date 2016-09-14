package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.EvictionStrategy;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator;
import static com.hazelcast.internal.eviction.EvictionStrategyProvider.getEvictionStrategy;
import static com.hazelcast.internal.eviction.impl.EvictionConfigHelper.checkEvictionConfig;

/**
 * Contains eviction specific functionality of a {@link QueryCacheRecordStore}
 */
public class EvictionOperator {

    private final QueryCacheRecordHashMap cache;
    private final EvictionConfig evictionConfig;
    private final MaxSizeChecker maxSizeChecker;
    private final EvictionPolicyEvaluator<Data, QueryCacheRecord> evictionPolicyEvaluator;
    private final EvictionChecker evictionChecker;
    private final EvictionStrategy<Data, QueryCacheRecord, QueryCacheRecordHashMap> evictionStrategy;
    private final EvictionListener<Data, QueryCacheRecord> listener;
    private final ClassLoader classLoader;

    public EvictionOperator(QueryCacheRecordHashMap cache,
                            QueryCacheConfig config,
                            EvictionListener<Data, QueryCacheRecord> listener,
                            ClassLoader classLoader) {
        this.cache = cache;
        this.evictionConfig = config.getEvictionConfig();
        this.maxSizeChecker = createCacheMaxSizeChecker();
        this.evictionPolicyEvaluator = createEvictionPolicyEvaluator();
        this.evictionChecker = createEvictionChecker();
        this.evictionStrategy = createEvictionStrategy();
        this.listener = listener;
        this.classLoader = classLoader;
    }

    boolean isEvictionEnabled() {
        return evictionStrategy != null && evictionPolicyEvaluator != null;
    }

    int evictIfRequired() {
        int evictedCount = 0;
        if (isEvictionEnabled()) {
            evictedCount = evictionStrategy.evict(cache, evictionPolicyEvaluator, evictionChecker, listener);
        }
        return evictedCount;
    }

    private MaxSizeChecker createCacheMaxSizeChecker() {
        return new MaxSizeChecker() {
            @Override
            public boolean isReachedToMaxSize() {
                return cache.size() > evictionConfig.getSize();
            }
        };
    }

    private EvictionPolicyEvaluator<Data, QueryCacheRecord> createEvictionPolicyEvaluator() {
        checkEvictionConfig(evictionConfig);
        return getEvictionPolicyEvaluator(evictionConfig, classLoader);
    }

    private EvictionStrategy<Data, QueryCacheRecord, QueryCacheRecordHashMap> createEvictionStrategy() {
        return getEvictionStrategy(evictionConfig);
    }

    private EvictionChecker createEvictionChecker() {
        return new MaxSizeEvictionChecker();
    }

    /**
     * Checks whether the eviction is startable.
     * It should be started when cache reaches the configured max-size.
     */
    private class MaxSizeEvictionChecker implements EvictionChecker {

        @Override
        public boolean isEvictionRequired() {
            return maxSizeChecker != null && maxSizeChecker.isReachedToMaxSize();
        }
    }
}
