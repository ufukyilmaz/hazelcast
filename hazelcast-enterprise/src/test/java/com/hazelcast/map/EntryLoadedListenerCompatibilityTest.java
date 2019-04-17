package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class EntryLoadedListenerCompatibilityTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "test";

    private HazelcastInstance version_3_10_node;
    private HazelcastInstance version_3_11_node1;
    private HazelcastInstance version_3_11_node2;

    private CompatibilityTestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Override
    public Config getConfig() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setClassName(TestMapLoader.class.getName());

        Config config = new Config();
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);

        return config;
    }

    @Test
    public void entry_loaded_listener_only_notified_after_upgrading_to_3_11_from_3_10() {
        String[] versions = new String[]{"3.10", "3.11", "3.11"};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        version_3_10_node = factory.newHazelcastInstance(getConfig());
        version_3_11_node1 = factory.newHazelcastInstance(getConfig());

        IMap<Object, Object> version_3_11_map = version_3_11_node1.getMap(MAP_NAME);
        final IMap<Object, Object> version_3_10_map = version_3_10_node.getMap(MAP_NAME);

        version_3_11_map.evictAll();

        final Version_3_11_LoadAndAddListener version_3_11_loadAndAddListener = new Version_3_11_LoadAndAddListener();
        version_3_11_map.addLocalEntryListener(version_3_11_loadAndAddListener);

        final Version_3_10_AddListener version_3_10_addListener = new Version_3_10_AddListener();
        version_3_10_map.addLocalEntryListener(version_3_10_addListener);

        version_3_11_map.loadAll(true);

        // 1. Cluster state 3.10 at this stage and we don't expect any LOAD
        //    event, we should only see ADD events
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Version clusterVersion = version_3_11_node1.getCluster().getClusterVersion();
                String msg = "clusterVersion{" + clusterVersion + "}, "
                        + version_3_11_loadAndAddListener.toString()
                        + ", " + version_3_10_addListener.toString();
                assertTrue(msg, version_3_11_loadAndAddListener.getAddEventCount() > 0);
                assertEquals(msg, 0, version_3_11_loadAndAddListener.getLoadEventCount());
                assertTrue(msg, version_3_10_addListener.getAddEventCount() > 0);
                assertEquals(msg, 1000, version_3_10_addListener.getAddEventCount()
                        + version_3_11_loadAndAddListener.getAddEventCount());
            }
        });

        version_3_11_node2 = factory.newHazelcastInstance(getConfig());
        version_3_10_node.shutdown();

        waitClusterForSafeState(version_3_11_node2);

        IMap<Object, Object> version_3_11_map2 = version_3_11_node2.getMap(MAP_NAME);

        // 2. Upgrade cluster state to 3.11 and reload map via map-loader.
        version_3_11_node2.getCluster().changeClusterVersion(Versions.V3_11);

        version_3_11_map.evictAll();
        version_3_11_loadAndAddListener.zeroCounters();

        version_3_11_map2.addLocalEntryListener(version_3_11_loadAndAddListener);

        version_3_11_map.loadAll(true);

        // 3. Cluster state 3.11 at this stage and we expect to see LOAD
        //    events, we shouldn't see any ADD event.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Version clusterVersion = version_3_11_node1.getCluster().getClusterVersion();
                String msg = "clusterVersion{" + clusterVersion + "}, "
                        + version_3_11_loadAndAddListener.toString()
                        + ", " + version_3_10_addListener.toString();
                assertEquals(msg, 0, version_3_11_loadAndAddListener.getAddEventCount());
                assertTrue(msg, version_3_11_loadAndAddListener.getLoadEventCount() > 0);
                assertEquals(msg, 1000, version_3_11_loadAndAddListener.getLoadEventCount());
            }
        });
    }

    static class Version_3_10_AddListener implements EntryAddedListener<Integer, Integer> {

        private final AtomicInteger addEventCount;

        Version_3_10_AddListener() {
            this.addEventCount = new AtomicInteger();
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Integer> event) {
            addEventCount.incrementAndGet();
        }

        public int getAddEventCount() {
            return addEventCount.get();
        }

        @Override
        public String toString() {
            return "Version_3_10_AddListener{"
                    + "addEventCount=" + addEventCount
                    + '}';
        }
    }

    static class Version_3_11_LoadAndAddListener implements EntryLoadedListener<Integer, Integer>,
            EntryAddedListener<Integer, Integer> {

        private final AtomicInteger loadEventCount;
        private final AtomicInteger addEventCount;

        Version_3_11_LoadAndAddListener() {
            this.loadEventCount = new AtomicInteger();
            this.addEventCount = new AtomicInteger();
        }

        public void zeroCounters() {
            loadEventCount.set(0);
            addEventCount.set(0);
        }

        @Override
        public void entryLoaded(EntryEvent<Integer, Integer> event) {
            loadEventCount.incrementAndGet();
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Integer> event) {
            addEventCount.incrementAndGet();
        }

        public int getAddEventCount() {
            return addEventCount.get();
        }

        public int getLoadEventCount() {
            return loadEventCount.get();
        }

        @Override
        public String toString() {
            return "Version_3_11_LoadAndAddListener{"
                    + "loadEventCount=" + loadEventCount
                    + ", addEventCount=" + addEventCount
                    + '}';
        }
    }

    static class TestMapLoader implements MapLoader<Integer, Integer> {

        AtomicInteger sequence = new AtomicInteger();

        TestMapLoader() {
        }

        @Override
        public Integer load(Integer key) {
            return sequence.incrementAndGet();
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                map.put(key, sequence.incrementAndGet());
            }
            return map;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            LinkedList<Integer> keys = new LinkedList<Integer>();
            for (int i = 0; i < 1000; i++) {
                keys.add(i);
            }
            return keys;
        }
    }

}
