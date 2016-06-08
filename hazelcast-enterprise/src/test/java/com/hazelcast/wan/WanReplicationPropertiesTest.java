package com.hazelcast.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;

/**
 * Tests for {@link com.hazelcast.enterprise.wan.replication.WanReplicationProperties}
 */

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanReplicationPropertiesTest extends HazelcastTestSupport {

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(WanReplicationProperties.class);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMustHaveConfig() {
        WanReplicationProperties.getProperty(WanReplicationProperties.GROUP_PASSWORD, new HashMap<String, Comparable>(), null);
    }
}
