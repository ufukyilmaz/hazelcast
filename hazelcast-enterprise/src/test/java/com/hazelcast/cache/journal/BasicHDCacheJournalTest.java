package com.hazelcast.cache.journal;

import com.hazelcast.cache.impl.journal.BasicCacheJournalTest;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
public class BasicHDCacheJournalTest extends BasicCacheJournalTest {
    @Override
    protected Config getConfig() {
        final Config config = getHDConfig();
        final int defaultPartitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        config.addEventJournalConfig(new EventJournalConfig().setCapacity(200 * defaultPartitionCount)
                                                             .setCacheName("default").setEnabled(true));
        final CacheSimpleConfig nonEvictingCache = new CacheSimpleConfig().setName("cache")
                                                                          .setInMemoryFormat(InMemoryFormat.NATIVE);
        nonEvictingCache.getEvictionConfig()
                        .setMaximumSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)
                        .setSize(Integer.MAX_VALUE);
        final CacheSimpleConfig evictingCache = new CacheSimpleConfig().setName("evicting")
                                                                       .setInMemoryFormat(InMemoryFormat.NATIVE);
        return config.addCacheConfig(nonEvictingCache).addCacheConfig(evictingCache);
    }
}
