package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Ignore(value = "https://github.com/hazelcast/hazelcast-enterprise/issues/1678")
public class BasicHDMapJournalTest extends BasicMapJournalTest {

    @Override
    protected Config getConfig() {
        final Config config = getHDConfig();
        config.addEventJournalConfig(new EventJournalConfig().setMapName("default").setEnabled(true));
        final MapConfig mapConfig = new MapConfig("mappy");
        final MapConfig expiringMap = new MapConfig("expiring").setTimeToLiveSeconds(1);
        return config.addMapConfig(mapConfig).addMapConfig(expiringMap);
    }
}
