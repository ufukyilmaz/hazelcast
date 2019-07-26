package com.hazelcast.wan.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.CustomWanPublisherConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.wan.WanReplicationTestSupport;

import javax.cache.expiry.ExpiryPolicy;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings({"WeakerAccess", "SameParameterValue", "UnusedReturnValue"})
public abstract class CacheWanReplicationTestSupport extends WanReplicationTestSupport {

    protected static final String DEFAULT_CACHE_NAME = "default";

    protected void initConfigA() {
        configA = createConfig("A", "confA-" + UUID.randomUUID() + "-", 5701, isNativeMemoryEnabled());
    }

    protected void initConfigB() {
        configB = createConfig("B", "confB-" + UUID.randomUUID() + "-", 5801, isNativeMemoryEnabled());
    }

    protected void initConfigC() {
        configC = createConfig("C", "confC-" + UUID.randomUUID() + "-", 5901, isNativeMemoryEnabled());
    }

    protected void setupReplicateFrom(Config fromConfig,
                                      String publisherId,
                                      String publisherClass,
                                      String setupName,
                                      String policy,
                                      String cacheName,
                                      String filter) {
        WanReplicationConfig wanConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
        }

        wanConfig.addCustomPublisherConfig(new CustomWanPublisherConfig()
                .setPublisherId(publisherId)
                .setClassName(publisherClass));

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName(setupName)
                .setMergePolicy(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }

        fromConfig.addWanReplicationConfig(wanConfig);
        fromConfig.getCacheConfig(cacheName).setWanReplicationRef(wanRef);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz,
                                      String setupName, String policy, String cacheName) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, cacheName, null);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz,
                                      String setupName, String policy, String cacheName, String filter) {
        WanReplicationConfig wanConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
        }
        wanConfig.addWanBatchReplicationPublisherConfig(targetCluster(toConfig, clusterSz));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicy(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }

        fromConfig.addWanReplicationConfig(wanConfig);
        fromConfig.getCacheConfig(cacheName).setWanReplicationRef(wanRef);
    }

    protected int createCacheDataIn(HazelcastInstance[] cluster, String cacheName, int start, int end,
                                    boolean removeBeforePut) {
        return createCacheDataIn(cluster, cacheName, start, end, removeBeforePut, null);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected int createCacheDataIn(HazelcastInstance[] cluster, String cacheName, int start, int end,
                                    boolean removeBeforePut, ExpiryPolicy expiryPolicy) {
        return createCacheDataIn(cluster, cacheName, start, end, removeBeforePut, expiryPolicy, false);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected int createCacheDataIn(HazelcastInstance[] cluster, String cacheName,
                                    int start, int end, boolean removeBeforePut, ExpiryPolicy expiryPolicy,
                                    boolean usePutAll) {
        ICache<Integer, String> myCache = getCacheFromRandomMember(cluster, cacheName);
        HashMap<Integer, String> putAllMap = usePutAll ? new HashMap<>() : null;

        for (; start < end; start++) {
            if (removeBeforePut) {
                myCache.remove(start);
            }
            final String value = getNode(cluster).getConfig().getGroupConfig().getName() + start;
            if (usePutAll) {
                putAllMap.put(start, value);
            } else if (expiryPolicy == null) {
                myCache.put(start, value);
            } else {
                myCache.put(start, value, expiryPolicy);
            }
        }
        if (usePutAll) {
            if (expiryPolicy == null) {
                myCache.putAll(putAllMap);
            } else {
                myCache.putAll(putAllMap, expiryPolicy);
            }
        }
        return myCache.size();
    }

    protected int removeCacheDataIn(HazelcastInstance[] cluster, String cacheName, int start, int end) {
        ICache<Integer, String> myCache = getCacheFromRandomMember(cluster, cacheName);
        for (; start < end; start++) {
            myCache.remove(start);
        }
        return myCache.size();
    }

    protected void setExpiryPolicyDataIn(HazelcastInstance[] cluster, String cacheName, int start, int end,
                                         ExpiryPolicy expiryPolicy) {
        ICache<Integer, String> myCache = getCacheFromRandomMember(cluster, cacheName);
        for (; start < end; start++) {
            myCache.setExpiryPolicy(start, expiryPolicy);
        }
    }

    protected boolean checkCacheDataInFrom(HazelcastInstance[] targetCluster, final String cacheName, final int start,
                                           final int end, HazelcastInstance[] sourceCluster) {
        final String sourceGroupName = getNode(sourceCluster).getConfig().getGroupConfig().getName();
        final ICache<Integer, String> cache = getNode(targetCluster).getCacheManager().getCache(cacheName);
        assertTrueEventually(() -> {
            for (int i = start; i < end; i++) {
                assertEquals(sourceGroupName + i, cache.get(i));
            }
        });

        return true;
    }

    protected boolean checkKeysNotIn(HazelcastInstance[] targetCluster, String cacheName,
                                     final int start, final int end) {
        final ICache<Integer, String> cache = getNode(targetCluster).getCacheManager().getCache(cacheName);
        assertTrueEventually(() -> {
            for (int i = start; i < end; i++) {
                assertFalse(cache.containsKey(i));
            }
        });

        return true;
    }

    protected boolean checkCacheDataSize(HazelcastInstance[] targetCluster, final String cacheName, final int size) {
        final ICache cache = getNode(targetCluster).getCacheManager().getCache(cacheName);
        assertTrueEventually(() -> assertEquals(size, cache.size()));

        return true;
    }

    protected void increaseHitCount(HazelcastInstance[] targetCluster, final String cacheName,
                                    final int start, final int end, final int repeat) {
        final ICache<Integer, String> cache = getNode(targetCluster).getCacheManager().getCache(cacheName);
        for (int i = start; i < end; i++) {
            for (int j = 0; j < repeat; j++) {
                cache.get(i);
            }
        }
    }

    private Config createConfig(String groupName, String instanceName, int port, boolean nativeMemoryEnabled) {
        final Config config = getConfig()
                .setInstanceName(instanceName);
        config.getGroupConfig()
              .setName(groupName);
        config.getNetworkConfig()
              .setPortAutoIncrement(false)
              .setPort(port);
        final CacheSimpleConfig cacheConfig = config.getCacheConfig(DEFAULT_CACHE_NAME);
        final EvictionConfig evictionConfig = new EvictionConfig();
        if (nativeMemoryEnabled) {
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
            evictionConfig.setSize(90)
                          .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);
        return config;
    }

    private ICache<Integer, String> getCacheFromRandomMember(HazelcastInstance[] cluster, String cacheName) {
        HazelcastInstance node = getNode(cluster);
        return node.getCacheManager().getCache(cacheName);
    }
}
