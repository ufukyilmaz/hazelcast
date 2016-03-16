package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class QueryCacheIMapEventHandlingTest extends HazelcastTestSupport {

    @Test
    public void testEvent_MERGED() throws Exception {
        HazelcastInstance member = createHazelcastInstance();
        String mapName = randomMapName();
        IEnterpriseMap map = (IEnterpriseMap) member.getMap(mapName);
        final QueryCache<Integer, Integer> cqc = map.getQueryCache("cqc", TruePredicate.INSTANCE, true);

        final int key = 1;
        final int existingValue = 1;
        final int mergingValue = 2;

        map.put(key, existingValue);

        executeMergeOperation(member, mapName, key, mergingValue);

        assertMergingValueInQueryCache(cqc, key, mergingValue);
    }

    private void assertMergingValueInQueryCache(final QueryCache<Integer, Integer> cqc, final int key, final int mergingValue) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(mergingValue, cqc.get(key).intValue());
            }
        });
    }

    protected void executeMergeOperation(HazelcastInstance member, String mapName, int key, int mergedValue) throws Exception {
        Node node = getNode(member);
        NodeEngineImpl nodeEngine = node.nodeEngine;
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        SerializationService serializationService = getSerializationService(member);

        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(mergedValue);
        EntryView entryView = createSimpleEntryView(keyData, valueData, Mockito.mock(Record.class));

        MergeOperation mergeOperation = new MergeOperation(mapName, keyData, entryView, new PassThroughMergePolicy());
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(SERVICE_NAME, mergeOperation, partitionId);
        future.get();
    }


    @Test
    public void testEvent_EXPIRED() throws Exception {
        HazelcastInstance member = createHazelcastInstance();
        String mapName = randomMapName();
        IEnterpriseMap map = (IEnterpriseMap) member.getMap(mapName);
        final QueryCache<Integer, Integer> cqc = map.getQueryCache("cqc", TruePredicate.INSTANCE, true);

        int key = 1;
        int value = 1;

        final CountDownLatch latch = new CountDownLatch(1);
        cqc.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        }, true);

        map.put(key, value, 1, SECONDS);

        latch.await();
        sleepSeconds(1);

        // map#get creates EXPIRED event.
        map.get(key);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, cqc.size());
            }
        });
    }

}
