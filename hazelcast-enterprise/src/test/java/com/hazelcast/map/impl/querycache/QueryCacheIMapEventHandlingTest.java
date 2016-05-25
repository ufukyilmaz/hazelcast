/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.Node;
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
import com.hazelcast.spi.serialization.SerializationService;
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
                Integer currentValue = cqc.get(key);
                assertEquals(mergingValue, (Object)currentValue);
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
