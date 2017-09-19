package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseSerializationNativeCompatibilityTest {

    private EnterpriseSerializationService serializationService;

    @Before
    public void setup() {
        EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
        serializationService = builder
                .setVersion(InternalSerializationService.VERSION_1)
                .addPortableFactory(TestSerializationConstants.PORTABLE_FACTORY_ID, new PortableTest.TestPortableFactory())
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES)))
                .build();
    }

    @After
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void testSampleEncodeDecode() {
        SerializationV1Dataserializable testData = SerializationV1Dataserializable.createInstanceWithNonNullFields();
        Data data = serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Dataserializable testDataFromSerializer = serializationService.toObject(data);
        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSampleEncodeDecode_with_null_arrays() {
        SerializationV1Dataserializable testData = new SerializationV1Dataserializable();
        Data data = serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Dataserializable testDataFromSerializer = serializationService.toObject(data);
        assertEquals(testData, testDataFromSerializer);
    }

    @Test
    public void testSamplePortableEncodeDecode() {
        SerializationV1Portable testData = SerializationV1Portable.createInstanceWithNonNullFields();
        Data data = serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Portable testDataFromSerializer = serializationService.toObject(data);
        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSamplePortableEncodeDecode_with_null_arrays() {
        SerializationV1Portable testDataw = SerializationV1Portable.createInstanceWithNonNullFields();
        serializationService.toData(testDataw, DataType.NATIVE);
        SerializationV1Portable testData = new SerializationV1Portable();
        Data data = serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Portable testDataFromSerializer = serializationService.toObject(data);
        assertEquals(testData, testDataFromSerializer);
    }
}
