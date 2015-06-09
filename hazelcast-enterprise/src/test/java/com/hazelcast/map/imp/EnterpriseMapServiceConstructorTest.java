package com.hazelcast.map.imp;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.impl.EnterpriseMapServiceConstructor;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for {@link com.hazelcast.map.impl.EnterpriseMapServiceConstructor}
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseMapServiceConstructorTest {

    @Test
    public void checkPrivateConstructor() {
        HazelcastTestSupport.assertUtilityConstructor(EnterpriseMapServiceConstructor.class);
    }

}
