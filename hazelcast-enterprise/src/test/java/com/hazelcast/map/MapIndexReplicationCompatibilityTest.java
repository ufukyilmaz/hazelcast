package com.hazelcast.map;

import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;

import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Compatibility test for replication of programmatically added indexes during
 * rolling upgrade from previous to current version
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class MapIndexReplicationCompatibilityTest extends HazelcastTestSupport {

    private static final String MAP_NAME = randomMapName();
    private TestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    // GIVEN
    //  - map with programmatically added index
    //  - previous member version as master
    // THEN
    //  - a current member version can join and index is created on current member
    @Test
    public void test_indexCreatedOnCurrentMember_whenPreviousClusterVersion() {
        factory = new CompatibilityTestHazelcastInstanceFactory(new String[] {
                PREVIOUS_CLUSTER_VERSION.toString(),
                CURRENT_CLUSTER_VERSION.toString(),
        });
        HazelcastInstance[] instances = new HazelcastInstance[2];
        // start previous member
        instances[0] = factory.newHazelcastInstance();
        IMap mapOn39Master = instances[0].getMap(MAP_NAME);
        mapOn39Master.addIndex(IndexType.HASH, "name");
        mapOn39Master.put("1", new Person("1"));

        // start current member to join on previous cluster version
        instances[1] = factory.newHazelcastInstance();
        IMap mapOn310 = instances[1].getMap(MAP_NAME);
        assertNotNull(mapOn310.get("1"));
        final MapServiceContext mapServiceContext = getMapServiceContext(instances[1]);
        // ensure the global index is created on the current version
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() {
                return mapServiceContext.getMapContainer(MAP_NAME).getIndexes().getIndexes().length;
            }
        }, 1);
    }

    // GIVEN
    //  - map with programmatically added index
    //  - cluster of a previous member version as master and one current member
    //  - previous member shuts down -> current member is master, still on previous cluster version
    // THEN
    //  - a previous member can join cluster
    @Test
    public void test_previousMemberCanJoin_whenCurrentMemberPresent_onPreviousClusterVersion() {
        factory = new CompatibilityTestHazelcastInstanceFactory(new String[] {
                PREVIOUS_CLUSTER_VERSION.toString(),
                CURRENT_CLUSTER_VERSION.toString(),
                PREVIOUS_CLUSTER_VERSION.toString(),
        });
        HazelcastInstance[] instances = new HazelcastInstance[3];
        // start previous member
        instances[0] = factory.newHazelcastInstance();
        IMap mapOn39Master = instances[0].getMap(MAP_NAME);
        mapOn39Master.addIndex(IndexType.HASH, "name");
        mapOn39Master.put("1", new Person("1"));

        // start current member to join on previous cluster version
        instances[1] = factory.newHazelcastInstance();
        IMap mapOn310 = instances[1].getMap(MAP_NAME);
        assertNotNull(mapOn310.get("1"));

        // shutdown previous so current becomes master
        instances[0].shutdown();
        waitClusterForSafeState(instances[1]);

        // startup a new previous member to verify it joins the cluster
        instances[2] = factory.newHazelcastInstance();
        waitClusterForSafeState(instances[1]);
        IMap mapOn39Member = instances[2].getMap(MAP_NAME);
        waitClusterForSafeState(instances[1]);
        assertNotNull(mapOn39Member.get("1"));
    }

    // GIVEN
    //  - map with programmatically added index
    //  - cluster of a previous member version as master and one current member
    //  - previous member shuts down -> current member is master
    //  - cluster version is upgraded to current
    // THEN
    //  - a current member can join cluster
    @Test
    public void testUpgradeToCurrenClusterVersion() {
        factory = new CompatibilityTestHazelcastInstanceFactory(new String[] {
                PREVIOUS_CLUSTER_VERSION.toString(),
                CURRENT_CLUSTER_VERSION.toString(),
                CURRENT_CLUSTER_VERSION.toString(),
        });
        HazelcastInstance[] instances = new HazelcastInstance[3];
        // start previous member
        instances[0] = factory.newHazelcastInstance();
        IMap mapOn39 = instances[0].getMap(MAP_NAME);
        mapOn39.addIndex(IndexType.HASH, "name");
        mapOn39.put("1", new Person("1"));

        // start current member to join on previous cluster version
        instances[1] = factory.newHazelcastInstance();
        IMap mapOn310 = instances[1].getMap(MAP_NAME);
        assertNotNull(mapOn310.get("1"));

        // shutdown previous so current becomes master
        instances[0].shutdown();
        waitClusterForSafeState(instances[1]);

        instances[1].getCluster().changeClusterVersion(CURRENT_CLUSTER_VERSION);
        assertEquals(CURRENT_CLUSTER_VERSION, instances[1].getCluster().getClusterVersion());

        // startup a new current member to verify it joins the cluster
        instances[2] = factory.newHazelcastInstance();
        waitClusterForSafeState(instances[1]);
        IMap map = instances[2].getMap(MAP_NAME);
        waitClusterForSafeState(instances[1]);
        assertNotNull(map.get("1"));
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine1.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }

    public interface IPerson {

        String getName();

        void setName(String name);
    }

    public static class Person implements IPerson, Serializable {
        private static final long serialVersionUID = 1L;
        private String name;

        public Person() {
        }

        public Person(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void setName(String name) {
            this.name = name;
        }
    }

}
