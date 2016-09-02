package com.hazelcast.security;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class SecurityUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor()  {
        assertUtilityConstructor(SecurityUtil.class);
    }
}
