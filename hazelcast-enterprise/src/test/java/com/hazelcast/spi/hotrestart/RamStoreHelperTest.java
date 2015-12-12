package com.hazelcast.spi.hotrestart;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class RamStoreHelperTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(RamStoreHelper.class);
    }
}
