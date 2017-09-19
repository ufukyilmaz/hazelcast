package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
public class BasicHDMapJournalTest extends BasicMapJournalTest {

    @Override
    protected Config getConfig() {
        int defaultPartitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setMapName("default")
                .setCapacity(200 * defaultPartitionCount);

        MapConfig mapConfig = new MapConfig("mappy");

        MapConfig expiringMap = new MapConfig("expiring")
                .setTimeToLiveSeconds(1);

        return getHDConfig()
                .addEventJournalConfig(eventJournalConfig)
                .addMapConfig(mapConfig)
                .addMapConfig(expiringMap);
    }
}
