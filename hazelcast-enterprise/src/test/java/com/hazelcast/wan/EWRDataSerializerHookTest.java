package com.hazelcast.wan;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.enterprise.wan.EWRDataSerializerHook}
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class EWRDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        EWRDataSerializerHook hook = new EWRDataSerializerHook();
        IdentifiedDataSerializable batchWanRep = hook.createFactory().create(EWRDataSerializerHook.BATCH_WAN_REP_EVENT);
        assertTrue(batchWanRep instanceof BatchWanReplicationEvent);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        EWRDataSerializerHook hook = new EWRDataSerializerHook();
        hook.createFactory().create(999);
    }

}
