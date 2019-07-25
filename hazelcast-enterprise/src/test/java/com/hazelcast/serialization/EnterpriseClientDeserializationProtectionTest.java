package com.hazelcast.serialization;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.serialization.ClientDeserializationProtectionTest;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseClientDeserializationProtectionTest extends ClientDeserializationProtectionTest {
    // content inherited from parent class
}
