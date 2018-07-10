package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.impl.mapstore.MapLoaderTest;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.map.filter.DummyMapWanFilter;
import com.hazelcast.wan.map.filter.NoFilterMapWanFilter;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapWanBatchReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Test(expected = InvalidConfigurationException.class)
    public void failStartupWhenEndpointsAreMisconfigured() {
        final String publisherSetup = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, publisherSetup, PassThroughMergePolicy.class.getName());

        for (WanPublisherConfig publisherConfig : configA.getWanReplicationConfig(publisherSetup).getWanPublisherConfigs()) {
            final Map<String, Comparable> properties = publisherConfig.getProperties();
            final String endpoints = (String) properties.get(ENDPOINTS.key());
            final String endpointsWithError = endpoints.replaceFirst("\\.", "\\.mumboJumbo\n");
            properties.put(ENDPOINTS.key(), endpointsWithError);
        }

        startClusterA();
        createDataIn(clusterA, "map", 1, 10);
    }

    @Test
    @Ignore
    public void recoverAfterTargetClusterFailure() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();

        createDataIn(clusterA, "map", 0, 1000);

        sleepSeconds(10);

        clusterA[0].shutdown();
        sleepSeconds(10);
        startClusterB();
        assertDataInFromEventually(clusterB, "map", 0, 1000, getNode(clusterA[1]).getConfig().getGroupConfig().getName());
    }

    @Test
    public void testMapWanFilter() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), DummyMapWanFilter.class.getName());
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 1, 10);
        assertKeysInEventually(clusterB, "map", 1, 2);
        assertKeysNotInEventually(clusterB, "map", 2, 10);
    }

    @Test
    public void mapWanEventFilter_prevents_replication_of_loaded_entries_by_default() {
        String mapName = "default";
        int loadedEntryCount = 1111;

        // 1. MapWanEventFilter is null to see default behaviour of filtering
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), null);

        // 2. Add map-loader to cluster-A
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setImplementation(new MapLoaderTest.DummyMapLoader(loadedEntryCount));
        configA.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        // 3. Start cluster-A and cluster-B
        startClusterA();
        startClusterB();

        // 4. Create map to trigger eager map-loading on cluster-A
        getMap(clusterA, mapName);

        // 5. Ensure no keys are replicated to cluster-B
        assertKeysNotInAllTheTime(clusterB, mapName, 0, loadedEntryCount);
    }

    @Test
    public void mapWanEventFilter_allows_replication_of_loaded_entries_when_customized() {
        String mapName = "default";
        int loadedEntryCount = 1111;

        // 1. Add customized MapWanEventFilter. This filter doesn't do any filtering.
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), NoFilterMapWanFilter.class.getName());

        // 2. Add map-loader to cluster-A
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setImplementation(new MapLoaderTest.DummyMapLoader(loadedEntryCount));
        configA.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        // 3. Start cluster-A and cluster-B
        startClusterA();
        startClusterB();

        // 4. Create map to trigger eager map-loading on cluster-A
        getMap(clusterA, mapName);

        // 5. Ensure all keys are replicated to cluster-B eventually.
        assertKeysInEventually(clusterB, mapName, 0, loadedEntryCount);
    }

    @Test
    public void testMigration() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());

        initCluster(singleNodeA, configA);
        createDataIn(singleNodeA, "map", 0, 1000);
        initCluster(singleNodeC, configA);

        initCluster(clusterB, configB);

        assertDataInFromEventually(clusterB, "map", 0, 1000, singleNodeC[0].getConfig().getGroupConfig().getName());
    }
}
