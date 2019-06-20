package com.hazelcast.internal.partition;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.ICache;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.internal.cluster.impl.MembershipUpdateCompatibilityTest.changeClusterVersionEventually;
import static com.hazelcast.spi.properties.GroupProperty.TCP_JOIN_PORT_TRY_COUNT;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getOldestKnownVersion;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class MigrationReplicationCompatibilityTest extends HazelcastTestSupport {

    private static final String HD_PREFIX = "hd-";

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(4);

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"Map", newValidators(mapValidator(), mapHDValidator(), mapLockValidator(), mapHDLockValidator())},
                {"Cache", newValidators(cacheValidator(), cacheHDValidator())},
                {"MultiMap", newValidators(multiMapValidator(), multiMapLockValidator())},
                {"Queue", newValidators(queueValidator())},
                {"Lock", newValidators(lockValidator())},
                {"All", newValidators(mapValidator(), mapHDValidator(), mapLockValidator(), cacheValidator(), cacheHDValidator(),
                        multiMapValidator(), queueValidator(), lockValidator())},
        });
    }

    @Parameter
    public String testName;

    @Parameter(1)
    public DataStructureValidator[] validators;

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMigrations_whenPreviousVersion_isMaster() {
        HazelcastInstance master = factory.newHazelcastInstance(createConfig());
        initValidators(master);

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, hz);
        }

        assertClusterSize(getKnownPreviousVersionsCount() + 1, master);
        waitClusterForSafeState(master);

        validate(master);
    }

    @Test
    public void testMigrations_whenLatestVersion_isMaster() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig());
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));
        initValidators(master);

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance hz : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, hz);

        }

        assertClusterSize(getKnownPreviousVersionsCount() + 1, master);
        waitClusterForSafeState(master);

        validate(master);
    }

    @Test
    public void testShutdown_whenPreviousVersion_isMaster() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        HazelcastInstance master = instances[0];
        initValidators(master);

        for (int i = 1; i < instances.length; i++) {
            instances[i].shutdown();
        }

        assertClusterSizeEventually(1, master);

        validate(master);
    }

    @Test
    public void testShutdown_whenLatestVersion_isMaster() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig());
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        initValidators(master);

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }

        assertClusterSizeEventually(1, master);

        validate(master);
    }

    @Test
    public void testShutdown_whenPreviousVersionMaster_shutdowns() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        HazelcastInstance instance = instances[instances.length - 1];
        initValidators(instance);

        instances[0].shutdown();

        assertClusterSizeEventually(instances.length - 1, instance);

        validate(instance);
    }

    @Test
    public void testShutdown_whenLatestVersionMaster_shutdowns() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig());
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        initValidators(instances[0]);

        master.shutdown();

        assertClusterSizeEventually(instances.length, instances[0]);

        validate(instances[0]);
    }

    @Test
    public void testTerminate_whenPreviousVersion_isMaster() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        HazelcastInstance master = instances[0];
        initValidators(master);

        for (int i = 1; i < instances.length; i++) {
            instances[i].getLifecycleService().terminate();

            assertClusterSizeEventually((instances.length - i), master);
            waitClusterForSafeState(master);
        }

        validate(master);
    }

    @Test
    public void testTerminate_whenLatestVersion_isMaster() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig());
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        initValidators(master);

        for (int i = 0; i < instances.length; i++) {
            HazelcastInstance instance = instances[i];
            instance.getLifecycleService().terminate();

            assertClusterSizeEventually((instances.length - i), master);
            waitClusterForSafeState(master);
        }

        validate(master);
    }

    @Test
    public void testTerminate_whenPreviousVersionMaster_terminates() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        HazelcastInstance instance = instances[instances.length - 1];
        initValidators(instance);

        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(instances.length - 1, instance);

        validate(instance);
    }

    @Test
    public void testTerminate_whenLatestVersionMaster_terminates() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig());
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        initValidators(instances[0]);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(instances.length, instances[0]);

        validate(instances[0]);
    }

    @Test
    public void testVersionUpgrade() {
        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());
        final HazelcastInstance latest = HazelcastInstanceFactory.newHazelcastInstance(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        initValidators(latest);

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }

        // start second latest version member
        HazelcastInstanceFactory.newHazelcastInstance(createConfig());

        assertClusterSizeEventually(2, latest);

        Version currentVersion = getNode(latest).getVersion().asVersion();
        changeClusterVersionEventually(latest, currentVersion);

        validate(latest);
    }

    @Test
    public void testMasterTerminate_AfterVersionUpgrade() {
        HazelcastInstance[] instances = factory.newInstances(createConfig(), getKnownPreviousVersionsCount());
        final HazelcastInstance latest = HazelcastInstanceFactory.newHazelcastInstance(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(getKnownPreviousVersionsCount() + 1, instance);
        }

        initValidators(latest);

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }

        // start latest version members
        HazelcastInstance instance = HazelcastInstanceFactory.newHazelcastInstance(createConfig());
        HazelcastInstance instance2 = HazelcastInstanceFactory.newHazelcastInstance(createConfig());

        assertClusterSizeEventually(3, latest);

        Version currentVersion = getNode(latest).getVersion().asVersion();
        changeClusterVersionEventually(latest, currentVersion);

        validate(latest);

        latest.getLifecycleService().terminate();
        waitAllForSafeState(instance, instance2);
    }

    private void validate(HazelcastInstance instance) {
        for (DataStructureValidator validator : validators) {
            validator.validate(instance);
        }
    }

    private void initValidators(HazelcastInstance instance) {
        for (DataStructureValidator validator : validators) {
            validator.init(instance);
        }
    }

    private static MultiMapLockValidator multiMapLockValidator() {
        return new MultiMapLockValidator("multi-map", lockKeys());
    }

    private static MapLockValidator mapHDLockValidator() {
        return new MapLockValidator(HD_PREFIX + "map", lockKeys());
    }

    private static MapLockValidator mapLockValidator() {
        return new MapLockValidator("map", lockKeys());
    }

    private static LockValidator lockValidator() {
        return new LockValidator(lockKeys());
    }

    private static QueueValidator queueValidator() {
        return new QueueValidator("queue");
    }

    private static MultiMapValidator multiMapValidator() {
        return new MultiMapValidator("multi-map");
    }

    private static CacheValidator cacheHDValidator() {
        return new CacheValidator(HD_PREFIX + "cache");
    }

    private static CacheValidator cacheValidator() {
        return new CacheValidator("cache");
    }

    private static MapValidator mapHDValidator() {
        return new MapValidator(HD_PREFIX + "map");
    }

    private static MapValidator mapValidator() {
        return new MapValidator("map");
    }

    private static String[] lockKeys() {
        String[] keys = new String[100];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = "key-" + i;
        }
        return keys;
    }

    private Config createConfig() {
        Config config = new Config();
        config.setProperty(TCP_JOIN_PORT_TRY_COUNT.getName(), String.valueOf(getKnownPreviousVersionsCount() + 2));

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(128, MemoryUnit.MEGABYTES));

        MapConfig hdMapConfig = new MapConfig(HD_PREFIX + "*").setInMemoryFormat(InMemoryFormat.NATIVE);
        config.addMapConfig(hdMapConfig);

        config.addCacheConfig(new CacheSimpleConfig().setName("cache"));

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(100);

        CacheSimpleConfig hdCacheConfig = new CacheSimpleConfig().setName(HD_PREFIX + "*")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
        config.addCacheConfig(hdCacheConfig);

        return config;
    }

    private interface DataStructureValidator {

        void init(HazelcastInstance instance);

        void validate(HazelcastInstance instance);
    }

    private static class MapValidator implements DataStructureValidator {

        final String name;

        MapValidator(String name) {
            this.name = name;
        }

        @Override
        public void init(HazelcastInstance instance) {
            IMap<Object, Object> map = instance.getMap(name);
            for (int i = 0; i < 100; i++) {
                map.put(i, toValue(i));
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            IMap<Object, Object> map = instance.getMap(name);
            for (int i = 0; i < 100; i++) {
                assertEquals(toValue(i), map.get(i));
            }
        }
    }

    private static class MultiMapValidator implements DataStructureValidator {

        final String name;

        MultiMapValidator(String name) {
            this.name = name;
        }

        @Override
        public void init(HazelcastInstance instance) {
            MultiMap<Object, Object> map = instance.getMultiMap(name);
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < (i + 1); j++) {
                    map.put(i, toValue(j));
                }
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            MultiMap<Object, Object> map = instance.getMultiMap(name);
            for (int i = 0; i < 100; i++) {
                Collection<Object> values = map.get(i);
                assertEquals((i + 1), values.size());
            }
        }
    }

    private static class CacheValidator implements DataStructureValidator {

        final String name;

        private CacheValidator(String name) {
            this.name = name;
        }

        @Override
        public void init(HazelcastInstance instance) {
            ICache<Object, Object> cache = instance.getCacheManager().getCache(name);
            for (int i = 0; i < 100; i++) {
                while (true) {
                    try {
                        cache.put(i, toValue(i));
                        break;
                    } catch (CacheNotExistsException ignored) {
                        sleepMillis(100);
                    }
                }
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            ICache<Object, Object> cache = instance.getCacheManager().getCache(name);
            for (int i = 0; i < 100; i++) {
                assertEquals(toValue(i), cache.get(i));
            }
        }
    }

    private static class QueueValidator implements DataStructureValidator {

        final String name;

        private QueueValidator(String name) {
            this.name = name;
        }

        @Override
        public void init(HazelcastInstance instance) {
            IQueue<Object> queue = instance.getQueue(name);
            for (int i = 0; i < 100; i++) {
                queue.offer(i);
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            IQueue<Object> queue = instance.getQueue(name);
            for (int i = 0; i < 100; i++) {
                assertEquals(i, queue.poll());
            }
        }
    }

    private static class LockValidator implements DataStructureValidator {

        final String[] keys;

        private LockValidator(String[] keys) {
            this.keys = keys;
        }

        @Override
        public void init(HazelcastInstance instance) {
            for (String key : keys) {
                ILock lock = instance.getLock(key);
                lock.lock();
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            for (String key : keys) {
                ILock lock = instance.getLock(key);
                assertTrue(lock.isLockedByCurrentThread());
                lock.unlock();
            }
        }
    }

    private static class MapLockValidator implements DataStructureValidator {

        final String name;
        final String[] keys;

        private MapLockValidator(String name, String[] keys) {
            this.name = name;
            this.keys = keys;
        }

        @Override
        public void init(HazelcastInstance instance) {
            IMap<Object, Object> map = instance.getMap(name);
            for (String key : keys) {
                map.lock(key);
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            IMap<Object, Object> map = instance.getMap(name);
            for (String key : keys) {
                map.unlock(key);
            }
        }
    }

    private static class MultiMapLockValidator implements DataStructureValidator {

        final String name;
        final String[] keys;

        private MultiMapLockValidator(String name, String[] keys) {
            this.name = name;
            this.keys = keys;
        }

        @Override
        public void init(HazelcastInstance instance) {
            MultiMap<Object, Object> map = instance.getMultiMap(name);
            for (String key : keys) {
                map.lock(key);
            }
        }

        @Override
        public void validate(HazelcastInstance instance) {
            MultiMap<Object, Object> map = instance.getMultiMap(name);
            for (String key : keys) {
                map.unlock(key);
            }
        }
    }

    private static DataStructureValidator[] newValidators(DataStructureValidator... validators) {
        return validators;
    }

    private static String toValue(int i) {
        return "value" + i;
    }
}
