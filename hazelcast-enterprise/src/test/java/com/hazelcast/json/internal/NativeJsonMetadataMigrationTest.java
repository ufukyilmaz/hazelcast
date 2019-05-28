package com.hazelcast.json.internal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeJsonMetadataMigrationTest extends JsonMetadataCreationMigrationTest {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true));
        config.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);
        return config;
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
        EnterpriseRecordStore enterpriseRecordStore = (EnterpriseRecordStore) mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        return enterpriseRecordStore.getMetadataStore().get(keyData);
    }
}
