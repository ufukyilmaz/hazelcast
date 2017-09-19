package com.hazelcast.cache.journal;

import com.hazelcast.cache.impl.journal.BasicCacheJournalTest;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicHDCacheJournalTest extends BasicCacheJournalTest {

    @Override
    protected Config getConfig() {
        int defaultPartitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCacheName("default")
                .setCapacity(200 * defaultPartitionCount);

        CacheSimpleConfig nonEvictingCache = new CacheSimpleConfig()
                .setName("cache")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        nonEvictingCache.getEvictionConfig()
                .setMaximumSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)
                .setSize(Integer.MAX_VALUE);

        CacheSimpleConfig evictingCache = new CacheSimpleConfig()
                .setName("evicting")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        return getHDConfig()
                .addEventJournalConfig(eventJournalConfig)
                .addCacheConfig(nonEvictingCache)
                .addCacheConfig(evictingCache);
    }
}
