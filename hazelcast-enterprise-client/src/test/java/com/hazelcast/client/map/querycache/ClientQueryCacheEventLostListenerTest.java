package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.EnterpriseClientMapProxyImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.TestSubscriberContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientQueryCacheEventLostListenerTest extends HazelcastTestSupport {

    private HazelcastInstance node;

    @Before
    public void setUp() throws Exception {
        tearDown();

        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");

        node = Hazelcast.newHazelcastInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testListenerNotified_onEventLoss() throws Exception {
        int count = 30;
        String mapName = randomString();
        String queryCacheName = randomString();

        IMap<Integer, Integer> mapNode = node.getMap(mapName);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IEnterpriseMap<Integer, Integer> mapClient = (IEnterpriseMap) client.getMap(mapName);
        setTestSequencer(mapClient, count);

        for (int i = 0; i < count; i++) {
            mapNode.put(i, i);
        }
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
            mapNode.remove(i);
        }

        assertOpenEventually(lostEventCount, 10);
    }


    private void setTestSequencer(IMap map, int eventCount) {
        EnterpriseClientMapProxyImpl proxy = (EnterpriseClientMapProxyImpl) map;
        QueryCacheContext queryCacheContext = proxy.getQueryContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }

}
