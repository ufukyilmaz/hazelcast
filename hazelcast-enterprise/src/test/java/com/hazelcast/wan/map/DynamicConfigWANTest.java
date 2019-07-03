package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MapUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigWANTest extends MapWanReplicationTestSupport {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Test
    public void testDynamicMapConfigWithWAN_smoke() {
        String mapName = randomMapName();
        String wanSetupName = "atob";

        setupReplicateFrom(configA, configB, clusterB.length, wanSetupName, PassThroughMergePolicy.class.getName());
        // disable WAN replication for the default map config (it's auto-enabled by the setupReplicateFrom())
        configA.getMapConfig("default").setWanReplicationRef(null);

        startClusterA();
        startClusterB();

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        wanRef.setName(wanSetupName);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setWanReplicationRef(wanRef);

        HazelcastInstance clusterAMember = clusterA[0];
        Config dynamicConfig = clusterAMember.getConfig();
        dynamicConfig.addMapConfig(mapConfig);

        Map<Integer, Integer> inputMap = MapUtil.createHashMap(10);
        for (int i = 0; i < 10; i++) {
            inputMap.put(i, i);
        }
        IMap<Integer, Integer> map = getMap(clusterA, mapName);
        map.putAll(inputMap);

        assertKeysInEventually(clusterB, mapName, 0, 10);
    }
}
