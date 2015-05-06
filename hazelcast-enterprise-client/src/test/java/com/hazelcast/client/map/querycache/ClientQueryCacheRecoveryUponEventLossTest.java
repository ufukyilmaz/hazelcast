package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.EnterpriseClientMapProxyImpl;
import com.hazelcast.client.map.querycache.subscriber.TestClientSubscriberContext;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class ClientQueryCacheRecoveryUponEventLossTest extends HazelcastTestSupport {

    @Before
    public void setUp() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();

    }

    @After
    public void tearDown() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testForceConsistency() throws Exception {
        String mapName = randomMapName("map");
        String queryCacheName = randomMapName("cache");
        Config config = new Config();

        HazelcastInstance node = Hazelcast.newHazelcastInstance(config);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) client.getMap(mapName);
        //set test sequencer to subscriber.
        int count = 30;
        setTestSequencer(map, 9);

        //expecting one lost event per partition.
        final CountDownLatch lossCount = new CountDownLatch(8);
        final QueryCache queryCache = map.getQueryCache(queryCacheName, new SqlPredicate("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                ((InternalQueryCache) queryCache).tryRecover();
                lossCount.countDown();

            }
        }, false);


        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertOpenEventually(lossCount, 10);
    }

    private void setTestSequencer(IMap map, int eventCount) {
        EnterpriseClientMapProxyImpl proxy = (EnterpriseClientMapProxyImpl) map;
        QueryCacheContext queryCacheContext = proxy.getQueryContext();
        queryCacheContext.setSubscriberContext(new TestClientSubscriberContext(queryCacheContext, eventCount, true));
    }
}
