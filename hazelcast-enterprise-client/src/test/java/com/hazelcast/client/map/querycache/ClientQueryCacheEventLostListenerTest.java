package com.hazelcast.client.map.querycache;

import com.hazelcast.client.map.impl.proxy.EnterpriseClientMapProxyImpl;
import com.hazelcast.client.map.querycache.subscriber.TestClientSubscriberContext;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheEventLostListenerTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance node;

    @Before
    public void setUp() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        node = factory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testListenerNotified_onEventLoss() throws Exception {
        int count = 30;
        String mapName = randomString();
        String queryCacheName = randomString();

        IMap<Integer, Integer> mapNode = node.getMap(mapName);

        HazelcastInstance client = factory.newHazelcastClient();

        IEnterpriseMap<Integer, Integer> mapClient = (IEnterpriseMap) client.getMap(mapName);
        setTestSequencer(mapClient, 9);

        // expecting one lost event publication per partition.
        final CountDownLatch lostEventCount = new CountDownLatch(1);
        final QueryCache queryCache = mapClient.getQueryCache(queryCacheName, new SqlPredicate("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                lostEventCount.countDown();
            }
        }, false);


        for (int i = 0; i < count; i++) {
            mapNode.put(i, i);
        }

        assertOpenEventually(lostEventCount);
    }


    private void setTestSequencer(IMap map, int eventCount) {
        EnterpriseClientMapProxyImpl proxy = (EnterpriseClientMapProxyImpl) map;
        QueryCacheContext queryCacheContext = proxy.getQueryContext();
        queryCacheContext.setSubscriberContext(new TestClientSubscriberContext(queryCacheContext, eventCount, true));
    }

}
