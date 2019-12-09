package com.hazelcast.cache;

import classloading.domain.Person;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.CountingWanPublisher;
import com.hazelcast.wan.WanConsumer;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
import static com.hazelcast.config.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;

/**
 * Test transfer of CacheConfig's with typed Caches along with EE features (HotRestart, WAN replication)
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseCacheTypesConfigTest extends CacheTypesConfigTest {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @Override
    CacheConfig createCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setTypes(String.class, Person.class);
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(new EvictionConfig().setSize(30)
                .setMaxSizePolicy(FREE_NATIVE_MEMORY_SIZE)
                .setEvictionPolicy(EvictionPolicy.LFU));
        cacheConfig.setWanReplicationRef(new WanReplicationRef("wan-replication",
                PassThroughMergePolicy.class.getName(),
                Collections.<String>emptyList(),
                false));
        cacheConfig.getHotRestartConfig().setEnabled(true);
        return cacheConfig;
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(16, MemoryUnit.MEGABYTES));
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig().setName("wan-replication");
        WanCustomPublisherConfig pc = new WanCustomPublisherConfig()
                .setPublisherId("target-cluster")
                .setClassName(CountingWanPublisher.class.getName());
        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig()
                .setClassName(NoopWanConsumer.class.getName());
        wanReplicationConfig.addCustomPublisherConfig(pc);
        wanReplicationConfig.setConsumerConfig(wanConsumerConfig);
        config.addWanReplicationConfig(wanReplicationConfig);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(hotRestartFolderRule.getBaseDir())
                .setClusterDataRecoveryPolicy(PARTIAL_RECOVERY_MOST_RECENT)
                .setDataLoadTimeoutSeconds(10)
                .setValidationTimeoutSeconds(10);
        return config;
    }

    public static class NoopWanConsumer implements WanConsumer {
        @Override
        public void init(String wanReplicationName, WanConsumerConfig config) {

        }

        @Override
        public void shutdown() {

        }
    }
}
