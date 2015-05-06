package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.TestSubscriberContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class QueryCacheRecoveryUponEventLossTest extends HazelcastTestSupport {

    @Test
    public void testForceConsistency() throws Exception {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "false");
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        mapConfig.setBackupCount(0);

        HazelcastInstance node = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        setTestSequencer(node, 9);
        setTestSequencer(node2, 9);

        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) node.getMap(mapName);
        IMap<Integer, Integer> map2 = node2.getMap(mapName);

        //set test sequencer to subscribers.
        int count = 30;

        //expecting one lost event per partition.
        final CountDownLatch lossCount = new CountDownLatch(10);
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

    private void setTestSequencer(HazelcastInstance instance, int eventCount) {
        Node node = getNode(instance);
        MapService service = (MapService) node.getNodeEngine().getService(MapService.SERVICE_NAME);
        EnterpriseMapServiceContext mapServiceContext
                = (EnterpriseMapServiceContext) service.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }
}
