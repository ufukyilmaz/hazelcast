package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.EmptyStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static com.hazelcast.internal.serialization.impl.EnterpriseSerializationUtil.allocateNativeData;
import static com.hazelcast.internal.serialization.impl.EnterpriseSerializationUtil.readDataInternal;
import static com.hazelcast.internal.serialization.impl.EnterpriseSerializationUtil.readNativeData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseSerializationUtilTest extends AbstractEnterpriseSerializationTest {

    @Before
    public void setUp() throws Exception {
        initMemoryManagerAndSerializationService();
    }

    @After
    public void tearDown() throws Exception {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(EnterpriseSerializationUtil.class);
    }

    @Test
    public void testReadDataInternal_shouldReturnPayloadFromNativeEnterpriseByteArrayObjectDataInput() throws Exception {
        EnterpriseObjectDataInput input = getEnterpriseObjectDataInput(getDefaultPayload(ByteOrder.LITTLE_ENDIAN));

        Data output = readDataInternal(input, DataType.NATIVE, memoryManager, false);

        assertNotNull(output);
        assertDataLengthAndContent(getDefaultPayload(ByteOrder.LITTLE_ENDIAN), output);
    }

    @Test
    public void testReadDataInternal_shouldReturnPayloadFromNativeEnterpriseUnsafeObjectDataInput() throws Exception {
        EnterpriseObjectDataInput input
                = new EnterpriseUnsafeObjectDataInput(getDefaultPayload(ByteOrder.nativeOrder()), 0, serializationService);

        Data output = readDataInternal(input, DataType.NATIVE, memoryManager, false);

        assertNotNull(output);
        assertDataLengthAndContent(getDefaultPayload(ByteOrder.nativeOrder()), output);
    }

    @Test
    public void testReadDataInternal_shouldReturnPayloadFromHeapEnterpriseByteArrayObjectDataInput() throws Exception {
        EnterpriseObjectDataInput input = getEnterpriseObjectDataInput(getDefaultPayload(ByteOrder.LITTLE_ENDIAN));

        Data output = readDataInternal(input, DataType.HEAP, memoryManager, false);

        assertNotNull(output);
        assertDataLengthAndContent(getDefaultPayload(ByteOrder.LITTLE_ENDIAN), output);
    }

    @Test
    public void testReadDataInternal_shouldReturnNullOnNullArrayLength() throws Exception {
        EnterpriseObjectDataInput input = getEnterpriseObjectDataInput(NULL_ARRAY_LENGTH_PAYLOAD);

        Data output = readDataInternal(input, DataType.NATIVE, memoryManager, false);

        assertNull(output);
    }

    @Test
    public void testReadDataInternal_shouldReturnNoPayloadOnSizeZero() throws Exception {
        byte[] payload = new byte[]{0, 0, 0, 0};
        EnterpriseObjectDataInput input = getEnterpriseObjectDataInput(payload);

        Data output = readDataInternal(input, DataType.NATIVE, memoryManager, false);

        assertNotNull(output);
        assertEquals(0, output.dataSize());
        assertEquals(0, output.totalSize());
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testReadDataInternal_shouldThrowIfCouldNotReadFourBytes() throws Exception {
        byte[] payload = new byte[]{0};
        EnterpriseObjectDataInput input = getEnterpriseObjectDataInput(payload);

        readDataInternal(input, DataType.NATIVE, memoryManager, false);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testReadDataInternal_shouldThrowIfMemoryManagerIsNull() throws Exception {
        readDataInternal(null, DataType.NATIVE, null, false);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testAllocateNativeData_shouldThrowIfMemoryManagerIsNull() throws Exception {
        allocateNativeData(null, null, 0, 0, false);
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testReadNativeData_shouldThrowOnNativeOOME() throws Exception {
        EnterpriseObjectDataInput input = mock(EnterpriseObjectDataInput.class);

        readNativeData(input, outOfMemoryMemoryManager, 0, false);
    }

    @Test
    public void testReadNativeData_shouldNotThrowWithReadToHeapOnOOME() throws Exception {
        EnterpriseObjectDataInput input = mock(EnterpriseObjectDataInput.class);

        Data output = readNativeData(input, outOfMemoryMemoryManager, 64, true);

        assertNotNull(output);
        assertEquals(64, output.totalSize());
    }

    @Test(expected = NativeOutOfMemoryError.class)
    public void testAllocateNativeData_shouldThrowOnNativeOOME() throws Exception {
        EnterpriseObjectDataInput input = mock(EnterpriseObjectDataInput.class);

        allocateNativeData(input, outOfMemoryMemoryManager, 0, 0, false);
    }

    @Test
    public void testAllocateNativeData_shouldThrowOnNativeOOME_withSkipBytesOnOome() throws Exception {
        EnterpriseObjectDataInput input = mock(EnterpriseObjectDataInput.class);

        try {
            allocateNativeData(input, outOfMemoryMemoryManager, 0, 1024, true);
            fail("Expected NativeOutOfMemoryError");
        } catch (NativeOutOfMemoryError expected) {
            EmptyStatement.ignore(expected);
        }

        verify(input).skipBytes(1024);
        verifyNoMoreInteractions(input);
    }
}
