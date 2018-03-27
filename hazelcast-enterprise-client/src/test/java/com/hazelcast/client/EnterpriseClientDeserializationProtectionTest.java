package com.hazelcast.client;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests if deserialization blacklisting works for Enterprise clients.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelTest.class })
public class EnterpriseClientDeserializationProtectionTest extends ClientDeserializationProtectionTest {
    // content inherited
}
