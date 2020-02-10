package com.hazelcast.json.internal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.query.impl.Metadata;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getBackupInstance;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeJsonMetadataCreationTest extends JsonMetadataCreationTest {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true));
        return config;
    }

    @Override
    protected InMemoryFormat getInMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }

    @Override
    protected Metadata getMetadata(String mapName, Object key, int replicaIndex) {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[] { null });
        HazelcastInstance instance = factory.getAllHazelcastInstances().iterator().next();
        InternalSerializationService serializationService = getSerializationService(instance);
        Data keyData = serializationService.toData(key);
        int partitionId = getPartitionService(instance).getPartitionId(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(getBackupInstance(instances, partitionId, replicaIndex));
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        EnterpriseRecordStore enterpriseRecordStore
                = (EnterpriseRecordStore) mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        return enterpriseRecordStore.getMetadataStore().get(keyData);
    }

    @Test
    public void testClearRemovesAllMetadata() {
        final int entryCount = 1000;
        final IMap map = instances[0].getMap(randomName());
        for (int i = 0; i < entryCount; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        map.clear();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < factory.getCount(); i++) {
                    for (int j = 0; j < entryCount; j++) {
                        assertNull(getMetadata(map.getName(), createJsonValue("key", j), i));
                    }
                }
            }
        });
    }

    @Test
    public void testDestroyRemovesAllMetadata() {
        final int entryCount = 1000;
        IMap map = instances[0].getMap(randomName());
        for (int i = 0; i < entryCount; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        map.destroy();
        for (int i = 0; i < factory.getCount(); i++) {
            for (int j = 0; j < entryCount; j++) {
                assertNull(getMetadata(map.getName(), createJsonValue("key", j), i));
            }
        }
    }
}
