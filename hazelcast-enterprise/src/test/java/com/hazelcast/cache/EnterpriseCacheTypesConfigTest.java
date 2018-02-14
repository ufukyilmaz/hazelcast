package com.hazelcast.cache;

import classloading.domain.Person;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationConsumer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;

/**
 * Test transfer of CacheConfig's with typed Caches along with EE features (HotRestart, WAN replication)
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseCacheTypesConfigTest extends CacheTypesConfigTest {

    static final AtomicInteger counter = new AtomicInteger();

    @Rule
    public TestName testName = new TestName();

    File baseDir;

    @Before
    public void before() {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()
                + "_" + randomString()));
        delete(baseDir);

        if (!baseDir.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
    }

    @After
    public void after() {
        factory.terminateAll();
        delete(baseDir);
    }

    @Override
    CacheConfig createCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setTypes(String.class, Person.class);
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(new EvictionConfig(30,
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE, EvictionPolicy.LFU));
        cacheConfig.setWanReplicationRef(new WanReplicationRef("wan-replication",
                PassThroughCacheMergePolicy.class.getName(),
                Collections.<String>emptyList(),
                false));
        cacheConfig.getHotRestartConfig().setEnabled(true);
        return cacheConfig;
    }

    @Override
    protected Config getConfig() {
        int sequenceNumber = counter.incrementAndGet();
        Config config = super.getConfig();
        config.setInstanceName("instance_" + sequenceNumber);
        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(16, MemoryUnit.MEGABYTES));
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig().setName("wan-replication");
        WanPublisherConfig wanPublisherConfig = new WanPublisherConfig().setGroupName("target-cluster")
                .setClassName(NoopWanPublisher.class.getName());
        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig().setClassName(NoopWanConsumer.class.getName());
        wanPublisherConfig.getProperties().put(WanReplicationProperties.GROUP_PASSWORD.key(), "password");
        wanReplicationConfig.addWanPublisherConfig(wanPublisherConfig);
        wanReplicationConfig.setWanConsumerConfig(wanConsumerConfig);
        config.addWanReplicationConfig(wanReplicationConfig);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(new File(baseDir, "instance_" + sequenceNumber))
                .setClusterDataRecoveryPolicy(PARTIAL_RECOVERY_MOST_RECENT)
                .setDataLoadTimeoutSeconds(10)
                .setValidationTimeoutSeconds(10);
        return config;
    }

    public static class NoopWanConsumer implements WanReplicationConsumer {
        @Override
        public void init(Node node, String wanReplicationName, WanConsumerConfig config) {

        }

        @Override
        public void shutdown() {

        }
    }

    public static class NoopWanPublisher implements WanReplicationPublisher, WanReplicationEndpoint {
        @Override
        public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {

        }

        @Override
        public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {

        }

        @Override
        public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {

        }

        @Override
        public void checkWanReplicationQueues() {

        }

        @Override
        public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public void removeBackup(WanReplicationEvent wanReplicationEvent) {

        }

        @Override
        public void putBackup(WanReplicationEvent wanReplicationEvent) {

        }

        @Override
        public PublisherQueueContainer getPublisherQueueContainer() {
            return null;
        }

        @Override
        public void addMapQueue(String key, int partitionId, WanReplicationEventQueue value) {

        }

        @Override
        public void addCacheQueue(String key, int partitionId, WanReplicationEventQueue value) {

        }

        @Override
        public void pause() {

        }

        @Override
        public void resume() {

        }

        @Override
        public LocalWanPublisherStats getStats() {
            return null;
        }

        @Override
        public void publishSyncEvent(WanSyncEvent syncRequest) {

        }

        @Override
        public void clearQueues() {

        }

        @Override
        public void collectReplicationData(String wanReplicationName, PartitionReplicationEvent event,
                                           Collection<ServiceNamespace> namespaces,
                                           EWRMigrationContainer migrationDataContainer) {

        }

        @Override
        public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {

        }
    }
}
