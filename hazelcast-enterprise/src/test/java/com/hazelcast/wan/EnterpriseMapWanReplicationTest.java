package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import org.junit.Before;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class EnterpriseMapWanReplicationTest extends WanReplicationTest {

    @Before
    public void setup() throws Exception {
        super.setup();
        disableElasticMemory(configA);
        disableElasticMemory(configB);
        disableElasticMemory(configC);
    }

    private void disableElasticMemory(Config config) {
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "false");
    }
}