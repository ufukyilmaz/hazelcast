package com.hazelcast.internal.serialization.impl;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.EnterpriseObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.spy;

/**
 * Iterates over all {@link DataSerializable} and {@link IdentifiedDataSerializable} classes
 * and checks if they have to implement {@link Versioned}.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseDataSerializableImplementsVersionedTest extends DataSerializableImplementsVersionedTest {

    @Override
    protected ObjectDataOutput getObjectDataOutput() {
        return spy(EnterpriseObjectDataOutput.class);
    }

    @Override
    protected ObjectDataInput getObjectDataInput() {
        return spy(EnterpriseObjectDataInput.class);
    }
}
