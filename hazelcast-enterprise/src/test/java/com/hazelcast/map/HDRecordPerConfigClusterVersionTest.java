package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDIndexConfig;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordPerConfigClusterVersionTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;
    @Parameterized.Parameter(1)
    public boolean perEntryStatsEnabled;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, perEntryStatsEnabled: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, true},
                {InMemoryFormat.NATIVE, false},
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    private static final int MEMBER_COUNT = 3;
    private static final int NUMBER_OF_KEYS = 1000;
    ;
    private static final String MAP_NAME = "migrated-map";
    private static final MemberVersion PREVIOUS_MEMBER_VERSION
            = MemberVersion.of(Versions.PREVIOUS_CLUSTER_VERSION.toString());
    private static final MemberVersion CURRENT_MEMBER_VERSION
            = MemberVersion.of(Versions.CURRENT_CLUSTER_VERSION.toString());

    private Config config;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void setUp() throws Exception {
        factory = createHazelcastInstanceFactory(MEMBER_COUNT);

        config = getHDIndexConfig(smallInstanceConfig());
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        mapConfig.setPerEntryStatsEnabled(perEntryStatsEnabled);
    }

    @Test
    public void populated_map_with_current_version_writable_and_readable_after_downgrade() {
        MemberVersion memberVersion = CURRENT_MEMBER_VERSION;

        // create cluster with CURRENT_MEMBER_VERSION
        for (int i = 0; i < MEMBER_COUNT; i++) {
            newHazelcastInstance(factory, memberVersion, config);
        }

        // populate map
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        for (HazelcastInstance instance : instances) {

            IMap<Integer, Integer> map = instance.getMap(MAP_NAME);
            for (int i = 0; i < NUMBER_OF_KEYS; i++) {
                map.set(i, i);
            }
        }

        // ensure no in-flight operations
        waitAllForSafeState(instances);

        // populate and read map after downgrade
        for (HazelcastInstance instance : instances) {
            // change cluster version to PREVIOUS_CLUSTER_VERSION
            instance.getCluster().changeClusterVersion(Versions.PREVIOUS_CLUSTER_VERSION);

            IMap<Integer, Integer> map = instance.getMap(MAP_NAME);
            for (int i = 0; i < NUMBER_OF_KEYS; i++) {
                map.set(i, i);
            }

            for (int i = 0; i < NUMBER_OF_KEYS; i++) {
                assertNotNull(map.get(i));
            }
        }
    }

    @Test
    public void map_data_is_available_after_migrations_when_cluster_version_is_previous_one() {
        // populate map
        MemberVersion memberVersion = PREVIOUS_MEMBER_VERSION;
        IMap<Integer, Integer> map = getMapOnNewNode(memberVersion);
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            map.set(i, i);
        }

        // read map entries
        map = getMapOnNewNode(memberVersion);
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            assertNotNull(map.get(i));
        }

        // read map entries
        map = getMapOnNewNode(memberVersion);
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            assertNotNull(map.get(i));
        }

        assertEquals(NUMBER_OF_KEYS, map.size());
    }

    @NotNull
    private IMap<Integer, Integer> getMapOnNewNode(MemberVersion version) {
        HazelcastInstance node = newHazelcastInstance(factory, version, config);
        return node.getMap(MAP_NAME);
    }
}
