package com.hazelcast.wan;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class EnterpriseMapWanReplicationTest extends WanReplicationTest {

    static {
        System.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "false");
    }

}