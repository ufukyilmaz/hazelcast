package com.hazelcast.wan.cache;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.WanDummyPublisher;
import com.hazelcast.wan.impl.WanReplicationService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.Caching;
import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;
import java.util.stream.Stream;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanCacheTestSupport.getOrCreateCache;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class WanCustomPublisherCacheTest extends HazelcastTestSupport {
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE},
                {BINARY},
                {OBJECT}
        });
    }

    @Parameter(0)
    public InMemoryFormat inMemoryFormat;

    private Cluster sourceCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @BeforeClass
    public static void setupClass() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupClass() {
        JsrTestUtil.cleanup();
    }

    @After
    public void cleanup() {
        Caching.getCachingProvider().close();
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 2,
                () -> inMemoryFormat == NATIVE ? getSmallInstanceHDConfig() : smallInstanceConfig()).setup();

        configureCache(sourceCluster);

        wanReplication = replicate()
                .from(sourceCluster)
                .withWanPublisher(WanDummyPublisher.class)
                .withSetupName(REPLICATION_NAME)
                .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();
    }

    @Test
    public void addUpdateRemoveEventsAreReplicated() {
        sourceCluster.startClusterAndWaitForSafeState();

        // ADD
        fillCache(sourceCluster, CACHE_NAME, 0, 100);
        ICache<Integer, String> cache = getOrCreateCache(sourceCluster, CACHE_NAME);
        assertQueueContents(100, cache);

        // UPDATE
        fillCache(0, 100, cache, "update-");
        assertQueueContents(200, cache);

        // REMOVE
        for (int i = 0; i < 100; i++) {
            cache.remove(i);
        }
        assertQueueContents(300, cache);
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig();
        if (inMemoryFormat == NATIVE) {
            evictionConfig.setSize(90);
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        }

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setEvictionConfig(evictionConfig)
                   .getMergePolicyConfig().setPolicy(PassThroughMergePolicy.class.getName());
    }


    private WanDummyPublisher getWanReplicationImpl(HazelcastInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        DelegatingWanScheme delegate = service.getWanReplicationPublishers(REPLICATION_NAME);
        return (WanDummyPublisher) delegate.getPublishers().iterator().next();
    }

    private void assertTotalQueueSize(final int expectedQueueSize) {
        HazelcastInstance[] members = sourceCluster.getMembers();
        Queue<WanEvent<Object>> eventQueue1 = getWanReplicationImpl(members[0]).getEventQueue();
        Queue<WanEvent<Object>> eventQueue2 = getWanReplicationImpl(members[1]).getEventQueue();
        assertTrueEventually(() -> assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size()));
    }

    private void assertQueueContents(int expectedQueueSize, ICache<Integer, String> cache) {
        HazelcastInstance[] members = sourceCluster.getMembers();
        Queue<WanEvent<Object>> eventQueue1 = getWanReplicationImpl(members[0]).getEventQueue();
        Queue<WanEvent<Object>> eventQueue2 = getWanReplicationImpl(members[1]).getEventQueue();
        assertTrueEventually(() -> assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size()));

        HashMap<Object, Object> actualData = new HashMap<>();
        Stream.of(members)
              .flatMap(i -> getWanReplicationImpl(i).getEventQueue().stream())
              .forEach(e -> {
                  assertEquals(CacheService.SERVICE_NAME, e.getServiceName());
                  assertEquals(cache.getName(), e.getObjectName());
                  Object eventObject = e.getEventObject();
                  assertNotNull(eventObject);

                  switch (e.getEventType()) {
                      case SYNC:
                          fail("Unexpected event type");
                          break;
                      case ADD_OR_UPDATE:
                          @SuppressWarnings("unchecked")
                          CacheEntryView<Object, Object> o = (CacheEntryView<Object, Object>) eventObject;
                          actualData.put(o.getKey(), o.getValue());
                          break;
                      case REMOVE:
                          actualData.remove(eventObject);
                          break;
                  }
              });

        assertEquals(cache.size(), actualData.size());

        for (Cache.Entry<Integer, String> entry : cache) {
            assertEquals(entry.getValue(), actualData.get(entry.getKey()));
        }
    }
}
