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
import com.hazelcast.enterprise.wan.WanReplicationConsumer;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.CountingWanEndpoint;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;

/**
 * Test transfer of CacheConfig's with typed Caches along with EE features (HotRestart, WAN replication)
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseCacheTypesConfigTest extends CacheTypesConfigTest {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

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
        Config config = super.getConfig();
        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(16, MemoryUnit.MEGABYTES));
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig().setName("wan-replication");
        WanPublisherConfig wanPublisherConfig = new WanPublisherConfig().setGroupName("target-cluster")
                .setClassName(CountingWanEndpoint.class.getName());
        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig().setClassName(NoopWanConsumer.class.getName());
        wanPublisherConfig.getProperties().put(WanReplicationProperties.GROUP_PASSWORD.key(), "password");
        wanReplicationConfig.addWanPublisherConfig(wanPublisherConfig);
        wanReplicationConfig.setWanConsumerConfig(wanConsumerConfig);
        config.addWanReplicationConfig(wanReplicationConfig);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(hotRestartFolderRule.getBaseDir())
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
}
