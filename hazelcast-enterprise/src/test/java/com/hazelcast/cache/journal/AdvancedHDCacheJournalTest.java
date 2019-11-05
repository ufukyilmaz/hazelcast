package com.hazelcast.cache.journal;

import com.hazelcast.cache.impl.journal.AdvancedCacheJournalTest;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AdvancedHDCacheJournalTest extends AdvancedCacheJournalTest {

    @Override
    protected Config getConfig() {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig()
                .setName("*").setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.getEvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)
                .setSize(Integer.MAX_VALUE);
        cacheConfig.setEventJournalConfig(new EventJournalConfig().setEnabled(true));

        return getHDConfig(super.getConfig())
                .addCacheConfig(cacheConfig);
    }
}
