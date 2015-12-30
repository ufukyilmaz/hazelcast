package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseSerializationNativeCompatibilityTest {

    private EnterpriseSerializationService serializationService;
    @Before
    public void setup() {
        EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
        serializationService = builder
                .setVersion(SerializationService.VERSION_1)
                .addPortableFactory(TestSerializationConstants.PORTABLE_FACTORY_ID, new PortableTest.TestPortableFactory())
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES)))
                .build();
    }

    @After
    public void tearDown() {
        serializationService.destroy();
    }

    @Test
    public void testSampleEncodeDecode() throws IOException {
        SerializationV1Dataserializable testData = SerializationV1Dataserializable.createInstanceWithNonNullFields();
        Data data = this.serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Dataserializable testDataFromSerializer = this.serializationService.toObject(data);
        Assert.assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSampleEncodeDecode_with_null_arrays() throws IOException {
        SerializationV1Dataserializable testData = new SerializationV1Dataserializable();
        Data data = this.serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Dataserializable testDataFromSerializer = this.serializationService.toObject(data);
        Assert.assertEquals(testData, testDataFromSerializer);
    }

    @Test
    public void testSamplePortableEncodeDecode() throws IOException {
        SerializationV1Portable testData = SerializationV1Portable.createInstanceWithNonNullFields();
        Data data = this.serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Portable testDataFromSerializer = this.serializationService.toObject(data);
        Assert.assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSamplePortableEncodeDecode_with_null_arrays() throws IOException {
        SerializationV1Portable testDataw = SerializationV1Portable.createInstanceWithNonNullFields();
        this.serializationService.toData(testDataw, DataType.NATIVE);
        SerializationV1Portable testData = new SerializationV1Portable();
        Data data = this.serializationService.toData(testData, DataType.NATIVE);
        SerializationV1Portable testDataFromSerializer = this.serializationService.toObject(data);
        Assert.assertEquals(testData, testDataFromSerializer);
    }
}
