package com.hazelcast.client.map.querycache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.map.impl.proxy.EnterpriseClientMapProxyImpl;
import com.hazelcast.client.map.querycache.subscriber.TestClientSubscriberContext;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.HDTestSupport.getEnterpriseMap;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheRecoveryUponEventLossTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testForceConsistency() {
        String mapName = randomMapName("map");
        String queryCacheName = randomMapName("cache");
        Config config = new Config();

        config.setProperty(PARTITION_COUNT.getName(), "1");
        factory.newHazelcastInstance(config);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IEnterpriseMap<Integer, Integer> map = getEnterpriseMap(client, mapName);
        // set test sequencer to subscriber
        int count = 30;
        setTestSequencer(map, 9);

        final QueryCache queryCache = map.getQueryCache(queryCacheName, new SqlPredicate("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                queryCache.tryRecover();

            }
        }, false);


        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(9, queryCache.size());
            }
        });
    }

    private void setTestSequencer(IMap map, int eventCount) {
        EnterpriseClientMapProxyImpl proxy = (EnterpriseClientMapProxyImpl) map;
        QueryCacheContext queryCacheContext = proxy.getQueryContext();
        queryCacheContext.setSubscriberContext(new TestClientSubscriberContext(queryCacheContext, eventCount, true));
    }
}
