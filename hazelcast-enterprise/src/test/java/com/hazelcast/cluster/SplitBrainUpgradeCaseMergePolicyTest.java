package com.hazelcast.cluster;

import com.hazelcast.cache.ICache;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertNodesVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Verifies that the new merge policies are not applied when the cluster version is still 3.9,
 * but the legacy merge policies are still working.
 * <p>
 * The whole test can be removed in 3.11, since it's just checking the RU compatibility between 3.9 and 3.10.
 */
// RU_COMPAT_3_9
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class SplitBrainUpgradeCaseMergePolicyTest extends AbstractSplitBrainUpgradeTest {

    private static final MemberVersion LEGACY_VERSION = MemberVersion.of("3.9");
    private static final MemberVersion NEW_VERSION = MemberVersion.of("3.10");

    private static final Version LEGACY_CLUSTER_VERSION = Versions.V3_9;
    private static final Version NEW_CLUSTER_VERSION = Versions.V3_10;

    private static final String LEGACY_DATA_STRUCTURE = "myLegacyDataStructure";
    private static final String NEW_DATA_STRUCTURE = "myNewDataStructure";

    @Parameters(name = "isLegacyCluster:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                true,
                false,
        });
    }

    @Parameter
    public boolean isLegacyCluster;

    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    @SuppressWarnings("deprecation")
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getName())
                .setBatchSize(100);

        Config config = super.config();

        // configure mergeable data structures (legacy merge policies)
        config.getMapConfig(LEGACY_DATA_STRUCTURE)
                .setMergePolicy(com.hazelcast.map.merge.PassThroughMergePolicy.class.getName());
        config.getCacheConfig(LEGACY_DATA_STRUCTURE)
                .setMergePolicy(com.hazelcast.cache.merge.PassThroughCacheMergePolicy.class.getName());
        config.getReplicatedMapConfig(LEGACY_DATA_STRUCTURE)
                .setMergePolicy(com.hazelcast.replicatedmap.merge.PassThroughMergePolicy.class.getName());

        // configure not mergeable data structures (new merge policies)
        config.getMapConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getCacheConfig(NEW_DATA_STRUCTURE)
                .setMergePolicy(mergePolicyConfig.getPolicy());
        config.getReplicatedMapConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getAtomicLongConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getAtomicReferenceConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getSetConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getListConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getQueueConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getRingbufferConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getMultiMapConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getCardinalityEstimatorConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getScheduledExecutorConfig(NEW_DATA_STRUCTURE)
                .setMergePolicyConfig(mergePolicyConfig);

        return config;
    }

    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        instances[0] = newHazelcastInstance(factory, isLegacyCluster ? LEGACY_VERSION : NEW_VERSION, config);
        instances[1] = newHazelcastInstance(factory, isLegacyCluster ? LEGACY_VERSION : NEW_VERSION, config);
        instances[2] = newHazelcastInstance(factory, NEW_VERSION, config);
        return instances;
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        // wait for sub-clusters to settle
        waitAllForSafeState(firstBrain);
        waitAllForSafeState(secondBrain);
        // make sure we are still at 3.9 cluster version when running a legacy cluster
        assertClusterVersion(firstBrain, isLegacyCluster ? LEGACY_CLUSTER_VERSION : NEW_CLUSTER_VERSION);
        assertClusterVersion(secondBrain, isLegacyCluster ? LEGACY_CLUSTER_VERSION : NEW_CLUSTER_VERSION);

        // create legacy data structures
        IMap<String, String> legacyMap = secondBrain[0].getMap(LEGACY_DATA_STRUCTURE);
        legacyMap.put("key", "value");

        ICache<String, String> legacyCache = secondBrain[0].getCacheManager().getCache(LEGACY_DATA_STRUCTURE);
        legacyCache.put("key", "value");

        ReplicatedMap<String, String> legacyReplicatedMap = secondBrain[0].getReplicatedMap(LEGACY_DATA_STRUCTURE);
        legacyReplicatedMap.put("key", "value");

        // create new data structures
        IMap<String, String> map = secondBrain[0].getMap(NEW_DATA_STRUCTURE);
        map.put("key", "value");

        ICache<String, String> cache = secondBrain[0].getCacheManager().getCache(NEW_DATA_STRUCTURE);
        cache.put("key", "value");

        ReplicatedMap<String, String> replicatedMap = secondBrain[0].getReplicatedMap(NEW_DATA_STRUCTURE);
        replicatedMap.put("key", "value");

        IAtomicLong atomicLong = secondBrain[0].getAtomicLong(NEW_DATA_STRUCTURE);
        atomicLong.set(42);

        IAtomicReference<String> atomicReference = secondBrain[0].getAtomicReference(NEW_DATA_STRUCTURE);
        atomicReference.set("value");

        ISet<String> set = secondBrain[0].getSet(NEW_DATA_STRUCTURE);
        set.add("value");

        IList<String> list = secondBrain[0].getList(NEW_DATA_STRUCTURE);
        list.add("value");

        IQueue<String> queue = secondBrain[0].getQueue(NEW_DATA_STRUCTURE);
        queue.add("value");

        Ringbuffer<String> ringbuffer = secondBrain[0].getRingbuffer(NEW_DATA_STRUCTURE);
        ringbuffer.add("value");

        MultiMap<String, String> multiMap = secondBrain[0].getMultiMap(NEW_DATA_STRUCTURE);
        multiMap.put("key", "value1");
        multiMap.put("key", "value2");

        CardinalityEstimator estimator = secondBrain[0].getCardinalityEstimator(NEW_DATA_STRUCTURE);
        estimator.add("value1");
        estimator.add("value2");

        IScheduledExecutorService scheduledExecutor = secondBrain[0].getScheduledExecutorService(NEW_DATA_STRUCTURE);
        scheduledExecutor.schedule(new HitchhikerTask(), 0, TimeUnit.SECONDS);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        // make sure we are still at 3.9 cluster version, when running a legacy cluster
        assertClusterVersion(instances, isLegacyCluster ? LEGACY_CLUSTER_VERSION : NEW_CLUSTER_VERSION);
        assertClusterSize(instances.length, instances[1]);
        assertNodesVersion(Arrays.copyOfRange(instances, 0, 1), isLegacyCluster ? LEGACY_VERSION : NEW_VERSION);
        assertNodesVersion(Arrays.copyOfRange(instances, 2, 2), NEW_VERSION);

        // assert that the legacy data structures have been merged
        IMap<String, String> legacyMap = instances[0].getMap(LEGACY_DATA_STRUCTURE);
        assertValue("value in IMap (legacy merge policy)", "value", "value", legacyMap.get("key"));

        ICache<String, String> legacyCache = instances[0].getCacheManager().getCache(LEGACY_DATA_STRUCTURE);
        assertValue("value in ICache (legacy merge policy)", "value", "value", legacyCache.get("key"));

        ReplicatedMap<String, String> legacyReplicatedMap = instances[0].getReplicatedMap(LEGACY_DATA_STRUCTURE);
        assertValue("value in ReplicatedMap (legacy merge policy)", "value", "value", legacyReplicatedMap.get("key"));

        // assert that the new data structures have not been merged in a legacy cluster
        IMap<String, String> map = instances[0].getMap(NEW_DATA_STRUCTURE);
        assertValue("value in IMap (new merge policy)", null, "value", map.get("key"));

        ICache<String, String> cache = instances[0].getCacheManager().getCache(NEW_DATA_STRUCTURE);
        assertValue("value in ICache (new merge policy)", null, "value", cache.get("key"));

        ReplicatedMap<String, String> replicatedMap = instances[0].getReplicatedMap(NEW_DATA_STRUCTURE);
        assertValue("value in ReplicatedMap (new merge policy)", null, "value", replicatedMap.get("key"));

        IAtomicLong atomicLong = instances[0].getAtomicLong(NEW_DATA_STRUCTURE);
        assertValue("IAtomicLong", 0L, 42L, atomicLong.get());

        IAtomicReference<String> atomicReference = instances[0].getAtomicReference(NEW_DATA_STRUCTURE);
        assertValue("IAtomicReference", null, "value", atomicReference.get());

        ISet<String> set = instances[0].getSet(NEW_DATA_STRUCTURE);
        assertSize("ISet", set.size());

        IList<String> list = instances[0].getList(NEW_DATA_STRUCTURE);
        assertSize("IList", list.size());

        IQueue<String> queue = instances[0].getQueue(NEW_DATA_STRUCTURE);
        assertSize("IQueue", queue.size());

        Ringbuffer<String> ringbuffer = instances[0].getRingbuffer(NEW_DATA_STRUCTURE);
        assertSize("Ringbuffer", ringbuffer.size());

        MultiMap<String, String> multiMap = instances[0].getMultiMap(NEW_DATA_STRUCTURE);
        assertValue("size of MultiMap", 0, 2, multiMap.size());

        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(NEW_DATA_STRUCTURE);
        assertValue("estimate of CardinalityEstimator", 0L, 2L, estimator.estimate());

        IScheduledExecutorService scheduledExecutor = instances[0].getScheduledExecutorService(NEW_DATA_STRUCTURE);
        int totalFutures = 0;
        for (List<IScheduledFuture<Object>> futures : scheduledExecutor.getAllScheduledFutures().values()) {
            totalFutures += futures.size();
        }
        assertSize("IScheduledExecutorService", totalFutures);
    }

    private void assertValue(String label, Object legacyValue, Object newValue, Object actualValue) {
        Object expectedValue = isLegacyCluster ? legacyValue : newValue;
        assertEquals(format("Expected %s to be %s, but was %s", label, expectedValue, actualValue), expectedValue, actualValue);
    }

    private void assertSize(String label, long actualSize) {
        assertValue("size of " + label, 0L, 1L, actualSize);
    }

    private static class HitchhikerTask implements Callable<Integer>, Serializable {

        @Override
        public Integer call() {
            return 42;
        }
    }
}
