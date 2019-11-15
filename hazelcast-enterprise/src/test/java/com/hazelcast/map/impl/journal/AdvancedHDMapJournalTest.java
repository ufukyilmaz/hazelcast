package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AdvancedHDMapJournalTest extends AdvancedMapJournalTest {

    @Override
    protected Config getConfig() {
        Config config = getHDConfig(super.getConfig());
        config.getMapConfig("default")
              .setEventJournalConfig(new EventJournalConfig().setEnabled(true));
        return config;
    }
}