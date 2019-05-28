package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemoryBlockDataInputTest extends AbstractEnterpriseSerializationTest {

    private static final byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private MemoryBlockDataInput in;

    @Before
    public void setUp() {
        initMemoryManagerAndSerializationService();
        Data nativeData = serializationService.convertData(new HeapData(INIT_DATA), DataType.NATIVE);
        in = new MemoryBlockDataInput((MemoryBlock) nativeData, 0, 0, serializationService);
    }

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Override
    protected boolean isUseNativeByteOrder() {
        return true;
    }

    @Test
    public void testByte_withPosition() throws Exception {
        Byte obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Byte expected = input.readByte(position);
        Byte actual = memoryBlockDataInput.readByte(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testByte() throws Exception {
        Byte obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Byte expected = input.readByte();
        Byte actual = memoryBlockDataInput.readByte();

        assertEquals(expected, actual);
    }

    @Test
    public void testBoolean_withPosition() throws Exception {
        Boolean obj = true;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Boolean expected = input.readBoolean(position);
        Boolean actual = memoryBlockDataInput.readBoolean(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testBoolean() throws Exception {
        Boolean obj = true;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Boolean expected = input.readBoolean();
        Boolean actual = memoryBlockDataInput.readBoolean();

        assertEquals(expected, actual);
    }

    @Test
    public void testShort_withPosition() throws Exception {
        Short obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Short expected = input.readShort(position);
        Short actual = memoryBlockDataInput.readShort(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testShort() throws Exception {
        Short obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Short expected = input.readShort();
        Short actual = memoryBlockDataInput.readShort();

        assertEquals(expected, actual);
    }

    @Test
    public void testChar_withPosition() throws Exception {
        Character obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Character expected = input.readChar(position);
        Character actual = memoryBlockDataInput.readChar(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testChar() throws Exception {
        Character obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Character expected = input.readChar();
        Character actual = memoryBlockDataInput.readChar();

        assertEquals(expected, actual);
    }

    @Test
    public void testInt_withPosition() throws Exception {
        int obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        int expected = input.readInt(position);
        int actual = memoryBlockDataInput.readInt(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testInt() throws Exception {
        int obj = 15;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        int expected = input.readInt();
        int actual = memoryBlockDataInput.readInt();

        assertEquals(expected, actual);
    }

    @Test
    public void testLong_withPosition() throws Exception {
        Long obj = 15L;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Long expected = input.readLong(position);
        Long actual = memoryBlockDataInput.readLong(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testLong() throws Exception {
        Long obj = 15L;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Long expected = input.readLong();
        Long actual = memoryBlockDataInput.readLong();

        assertEquals(expected, actual);
    }

    @Test
    public void testFloat_withPosition() throws Exception {
        Float obj = 15f;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Float expected = input.readFloat(position);
        Float actual = memoryBlockDataInput.readFloat(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testFloat() throws Exception {
        Float obj = 15f;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Float expected = input.readFloat();
        Float actual = memoryBlockDataInput.readFloat();

        assertEquals(expected, actual);
    }

    @Test
    public void testDouble_withPosition() throws Exception {
        Double obj = 15d;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        int position = input.position();
        assertEquals(memoryBlockDataInput.position(), position);

        Double expected = input.readDouble(position);
        Double actual = memoryBlockDataInput.readDouble(position);

        assertEquals(expected, actual);
    }

    @Test
    public void testDouble() throws Exception {
        Double obj = 15d;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        EnterpriseBufferObjectDataInput input = createInput(obj);

        input.position(HeapData.DATA_OFFSET);
        memoryBlockDataInput.position(HeapData.DATA_OFFSET);

        assertEquals(memoryBlockDataInput.position(), input.position());

        Double expected = input.readDouble();
        Double actual = memoryBlockDataInput.readDouble();

        assertEquals(expected, actual);
    }

    @Test
    public void testSkip() {
        long obj = 0x11121314L;
        int skipBytes = 4;
        MemoryBlockDataInput memoryBlockDataInput = (MemoryBlockDataInput) createNativeInput(obj);
        memoryBlockDataInput.skip(skipBytes);

        assertEquals(memoryBlockDataInput.position(), HeapData.DATA_OFFSET + skipBytes);
    }

    @Test
    public void testReadForBOffLen() throws Exception {
        int read = in.read(INIT_DATA, 0, 5);
        assertEquals(5, read);
    }

    @Test(expected = NullPointerException.class)
    public void testReadForBOffLen_null_array() throws Exception {
        in.read(null, 0, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadForBOffLen_negativeLen() throws Exception {
        in.read(INIT_DATA, 0, -11);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadForBOffLen_negativeOffset() throws Exception {
        in.read(INIT_DATA, -10, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadForBOffLen_Len_LT_Bytes() throws Exception {
        in.read(INIT_DATA, 0, INIT_DATA.length + 1);
    }

    @Test
    public void testReadForBOffLen_pos_gt_size() throws Exception {
        Data nativeData = serializationService.convertData(new HeapData(INIT_DATA), DataType.NATIVE);
        in = new MemoryBlockDataInput((MemoryBlock) nativeData, 100, 0, serializationService);

        int read = in.read(INIT_DATA, 0, 1);
        assertEquals(-1, read);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInit() {
        in.init(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClear() {
        in.clear();
    }

    private EnterpriseBufferObjectDataInput createNativeInput(Object obj) {
        Data nativeData = serializationService.toData(obj, DataType.NATIVE);
        EnterpriseUnsafeInputOutputFactory factory = new EnterpriseUnsafeInputOutputFactory();
        return factory.createInput(nativeData, serializationService);
    }

    private EnterpriseBufferObjectDataInput createInput(Object obj) {
        Data data = serializationService.toData(obj, DataType.HEAP);
        EnterpriseUnsafeInputOutputFactory factory = new EnterpriseUnsafeInputOutputFactory();
        return factory.createInput(data, serializationService);
    }

    @Test
    public void testGetSerializationService() {
        assertEquals(serializationService, in.getSerializationService());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCopyToMemoryBlock() throws Exception {
        in.copyToMemoryBlock(null, 0, 0);
    }

    @Test
    public void testGetClassLoader() {
        assertEquals(serializationService.getClassLoader(), in.getClassLoader());
    }

    @Test
    public void testToString() {
        assertNotNull(in.toString());
    }
}
