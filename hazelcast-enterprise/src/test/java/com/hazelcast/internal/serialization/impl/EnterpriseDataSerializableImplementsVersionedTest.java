package com.hazelcast.internal.serialization.impl;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.EnterpriseObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Iterates over all {@link DataSerializable} and {@link IdentifiedDataSerializable} classes
 * and checks if they have to implement {@link Versioned}.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseDataSerializableImplementsVersionedTest extends DataSerializableImplementsVersionedTest {

    private EnterpriseSerializationService serializationService = new EnterpriseSerializationServiceBuilder().build();

    @Override
    protected ObjectDataOutput getObjectDataOutput() {
        EnterpriseObjectDataOutput output = spy(EnterpriseObjectDataOutput.class);
        when(output.getSerializationService()).thenReturn(serializationService);
        return output;
    }

    @Override
    protected ObjectDataInput getObjectDataInput() {
        return spy(EnterpriseObjectDataInput.class);
    }
}
