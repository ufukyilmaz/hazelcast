package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.HDTestSupport;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
public class BasicHDMapJournalTest extends BasicMapJournalTest {
    @Override
    protected Config getConfig() {
        final Config config = HDTestSupport.getHDConfig();
        config.addEventJournalConfig(new EventJournalConfig().setMapName("default").setEnabled(true));
        final MapConfig mapConfig = new MapConfig("mappy");
        final MapConfig expiringMap = new MapConfig("expiring").setTimeToLiveSeconds(1);
        return config.addMapConfig(mapConfig).addMapConfig(expiringMap);
    }
}
