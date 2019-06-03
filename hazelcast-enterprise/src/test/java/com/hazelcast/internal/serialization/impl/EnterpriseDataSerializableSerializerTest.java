package com.hazelcast.internal.serialization.impl;

import com.hazelcast.cache.hidensity.operation.HiDensityCacheDataSerializerHook;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.createHeader;
import static com.hazelcast.version.Version.UNKNOWN;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseDataSerializableSerializerTest extends HazelcastTestSupport {

    private static final byte MAJOR_VERSION_BYTE = 3;
    private static final byte MINOR_VERSION_BYTE = 8;
    private static final Version CLUSTER_VERSION = Version.of(MAJOR_VERSION_BYTE, MINOR_VERSION_BYTE);

    private VersionedObjectDataInput input;
    private VersionedObjectDataOutput output;

    private EnterpriseDataSerializableSerializer enterpriseSerializer;

    @Before
    public void initSerializer() {
        input = mock(VersionedObjectDataInput.class);
        output = mock(VersionedObjectDataOutput.class);

        Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();
        factories.put(1, new TestDataSerializableFactory());
        factories.put(2, new TestVersionedDataSerializableFactory());

        enterpriseSerializer = new EnterpriseDataSerializableSerializer(factories, getClass().getClassLoader(),
                new TestClusterVersionAware());
    }

    @After
    public void tearDown() {
        enterpriseSerializer.destroy();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenFactoryAlreadyRegistered_thenThrowException() throws Exception {
        Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();
        factories.put(HiDensityCacheDataSerializerHook.F_ID, new TestDataSerializableFactory());

        new EnterpriseDataSerializableSerializer(factories, getClass().getClassLoader(), new TestClusterVersionAware());
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRead_whenClassIsNotInstantiable_thenThrowException() throws Exception {
        enterpriseSerializer.read(input, TestNotInstantiableDataSerializable.class);
    }

    @Test
    public void testRead_byteFormat_DS_unversioned_withClass() throws Exception {
        testRead_byteFormat_DS_unversioned(true);
    }

    @Test
    public void testRead_byteFormat_DS_unversioned() throws Exception {
        testRead_byteFormat_DS_unversioned(false);
    }

    private void testRead_byteFormat_DS_unversioned(boolean withClass) throws Exception {
        TestDataSerializable original = new TestDataSerializable(123);
        when(input.getClassLoader()).thenReturn(getClass().getClassLoader());
        when(input.readByte()).thenReturn(createHeader(false, false));
        when(input.readUTF()).thenReturn(original.getClass().getName());
        when(input.readInt()).thenReturn(original.value);
        when(input.getVersion()).thenReturn(Version.UNKNOWN);

        DataSerializable deserialized;
        if (withClass) {
            deserialized = enterpriseSerializer.read(input, TestDataSerializable.class);
        } else {
            deserialized = enterpriseSerializer.read(input);
        }

        InOrder calls = inOrder(input);
        calls.verify(input).readByte();
        calls.verify(input).readUTF();
        calls.verify(input).getVersion();
        calls.verify(input).setVersion(UNKNOWN);
        if (!withClass) {
            calls.verify(input).getClassLoader();
        }
        calls.verify(input).readInt();
        calls.verify(input).setVersion(UNKNOWN);
        verifyNoMoreInteractions(input);

        assertInstanceOf(TestDataSerializable.class, deserialized);
        assertEquals(original.value, ((TestDataSerializable) deserialized).value);
    }

    @Test
    public void testRead_byteFormat_DS_versioned_withClass() throws Exception {
        testRead_byteFormat_DS_versioned(false);
    }

    @Test
    public void testRead_byteFormat_DS_versioned() throws Exception {
        testRead_byteFormat_DS_versioned(false);
    }

    public void testRead_byteFormat_DS_versioned(boolean withClass) throws Exception {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        when(input.getClassLoader()).thenReturn(getClass().getClassLoader());
        when(input.readByte())
                .thenReturn(createHeader(false, true))
                .thenReturn(MAJOR_VERSION_BYTE)
                .thenReturn(MINOR_VERSION_BYTE);
        when(input.readUTF()).thenReturn(original.getClass().getName());
        when(input.readInt()).thenReturn(original.value);
        when(input.getVersion()).thenReturn(UNKNOWN);

        DataSerializable deserialized;
        if (withClass) {
            deserialized = enterpriseSerializer.read(input, TestVersionedDataSerializable.class);
        } else {
            deserialized = enterpriseSerializer.read(input);
        }

        InOrder calls = inOrder(input);
        calls.verify(input).readByte();
        calls.verify(input).readUTF();
        calls.verify(input).getVersion();
        calls.verify(input, times(2)).readByte();
        calls.verify(input).setVersion(CLUSTER_VERSION);
        calls.verify(input).getClassLoader();
        calls.verify(input).readInt();
        calls.verify(input).setVersion(UNKNOWN);
        verifyNoMoreInteractions(input);

        assertInstanceOf(TestVersionedDataSerializable.class, deserialized);
        assertEquals(original.value, ((TestVersionedDataSerializable) deserialized).value);
    }

    @Test(expected = IOException.class)
    public void testRead_byteFormat_DS_withIOException() throws Exception {
        when(input.readByte())
                .thenReturn(createHeader(false, true))
                .thenThrow(new IOException("expected"));

        enterpriseSerializer.read(input);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRead_byteFormat_DS_withHazelcastSerializationException() throws Exception {
        when(input.readByte())
                .thenReturn(createHeader(false, true))
                .thenThrow(new HazelcastSerializationException("expected"));

        enterpriseSerializer.read(input);
    }

    @Test(expected = HazelcastException.class)
    public void testRead_byteFormat_DS_withHazelcastException() throws Exception {
        when(input.readByte())
                .thenReturn(createHeader(false, true))
                .thenThrow(new HazelcastException("expected"));

        enterpriseSerializer.read(input);
    }

    @Test
    public void testRead_byteFormat_IDS_versioned_withClass() throws Exception {
        testRead_byteFormat_IDS_versioned(true);
    }

    @Test
    public void testRead_byteFormat_IDS_versioned() throws Exception {
        testRead_byteFormat_IDS_versioned(false);
    }

    private void testRead_byteFormat_IDS_versioned(boolean withClass) throws Exception {
        TestUncompressableIdentifiedDataSerializable original = new TestUncompressableIdentifiedDataSerializable(123);
        when(input.readByte())
                .thenReturn(createHeader(true, true))
                .thenReturn(MAJOR_VERSION_BYTE)
                .thenReturn(MINOR_VERSION_BYTE);
        when(input.readInt())
                .thenReturn(original.getFactoryId())
                .thenReturn(original.getClassId())
                .thenReturn(original.value);
        when(input.getVersion()).thenReturn(UNKNOWN);

        DataSerializable deserialized;
        if (withClass) {
            deserialized = enterpriseSerializer.read(input, TestUncompressableIdentifiedDataSerializable.class);
        } else {
            deserialized = enterpriseSerializer.read(input);
        }

        InOrder calls = inOrder(input);
        calls.verify(input).readByte();
        calls.verify(input, times(2)).readInt();
        calls.verify(input).getVersion();
        calls.verify(input, times(2)).readByte();
        calls.verify(input).setVersion(CLUSTER_VERSION);
        calls.verify(input).readInt();
        calls.verify(input).setVersion(UNKNOWN);
        verifyNoMoreInteractions(input);

        assertInstanceOf(TestUncompressableIdentifiedDataSerializable.class, deserialized);
        assertEquals(original.value, ((TestUncompressableIdentifiedDataSerializable) deserialized).value);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRead_byteFormat_IDS_whenInvalidFactoryId_thenThrowException() throws Exception {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);
        when(input.readByte())
                .thenReturn(createHeader(true, false))
                .thenReturn(Byte.MIN_VALUE)
                .thenReturn((byte) original.getClassId());
        when(input.readInt()).thenReturn(original.value);

        enterpriseSerializer.read(input);
    }

    @Test(expected = IOException.class)
    public void testRead_byteFormat_IDS_withIOException() throws Exception {
        when(input.readByte()).thenReturn(createHeader(true, false));
        when(input.readInt()).thenThrow(new IOException("expected"));

        enterpriseSerializer.read(input);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRead_byteFormat_IDS_withHazelcastSerializationException() throws Exception {
        when(input.readByte()).thenReturn(createHeader(true, false));
        when(input.readInt()).thenThrow(new HazelcastSerializationException("expected"));

        enterpriseSerializer.read(input);
    }

    @Test(expected = HazelcastException.class)
    public void testRead_byteFormat_IDS_withHazelcastException() throws Exception {
        when(input.readByte()).thenReturn(createHeader(true, false));
        when(input.readInt()).thenThrow(new HazelcastException("expected"));

        enterpriseSerializer.read(input);
    }

    @Test
    public void testWrite_byteFormat_DS_unversioned() throws Exception {
        TestDataSerializable original = new TestDataSerializable(123);
        when(output.getVersion()).thenReturn(UNKNOWN);

        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).getVersion();
        calls.verify(output).setVersion(UNKNOWN);
        calls.verify(output).writeByte(createHeader(false, false));
        calls.verify(output).writeUTF(TestDataSerializable.class.getName());
        calls.verify(output).writeInt(original.value);
        calls.verify(output).setVersion(UNKNOWN);
        verifyNoMoreInteractions(output);
    }

    @Test
    public void testWrite_byteFormat_DS_versioned() throws Exception {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).setVersion(CLUSTER_VERSION);
        calls.verify(output).writeByte(createHeader(false, true));
        calls.verify(output).writeUTF(TestVersionedDataSerializable.class.getName());
        calls.verify(output).writeInt(original.value);
        // Mockito calls a hash method on the proxy, so we cannot verifyNoMoreInteractions()
    }

    @Test
    public void testWrite_byteFormat_IDS_unversioned() throws Exception {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);
        when(output.getVersion()).thenReturn(UNKNOWN);
        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).getVersion();
        calls.verify(output).setVersion(UNKNOWN);
        calls.verify(output).writeByte(createHeader(true, false));
        calls.verify(output, times(2)).writeInt(original.getFactoryId());
        calls.verify(output).writeInt(original.value);
        calls.verify(output).setVersion(UNKNOWN);
        verifyNoMoreInteractions(output);
    }

    @Test
    public void testWrite_byteFormat_IDS_versioned() throws Exception {
        TestUncompressableIdentifiedDataSerializable original = new TestUncompressableIdentifiedDataSerializable(123);
        when(output.getVersion()).thenReturn(UNKNOWN);
        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).getVersion();
        calls.verify(output).setVersion(CLUSTER_VERSION);
        calls.verify(output).writeByte(createHeader(true, true));
        calls.verify(output).writeInt(original.getFactoryId());
        calls.verify(output).writeInt(original.getClassId());
        calls.verify(output).writeByte(MAJOR_VERSION_BYTE);
        calls.verify(output).writeByte(MINOR_VERSION_BYTE);
        calls.verify(output).writeInt(original.value);
        calls.verify(output).setVersion(UNKNOWN);
        verifyNoMoreInteractions(output);
    }

    private static class TestDataSerializableFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == 1) {
                return new TestIdentifiedDataSerializable();
            }
            if (typeId == 311) {
                return new TestUncompressableIdentifiedDataSerializable();
            }
            throw new RuntimeException("Unsupported typeId: " + typeId);
        }
    }

    private static class TestVersionedDataSerializableFactory extends TestDataSerializableFactory
            implements VersionedDataSerializableFactory {

        @Override
        public IdentifiedDataSerializable create(int typeId, Version version) {
            return create(typeId);
        }
    }

    private static class TestClusterVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return CLUSTER_VERSION;
        }
    }

    private static class TestDataSerializable implements DataSerializable {

        private int value;

        TestDataSerializable() {
        }

        TestDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }
    }

    private static class TestVersionedDataSerializable implements DataSerializable, Versioned {

        private int value;

        TestVersionedDataSerializable() {
        }

        TestVersionedDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }
    }

    private static class TestIdentifiedDataSerializable implements IdentifiedDataSerializable {

        private int value;

        TestIdentifiedDataSerializable() {
        }

        TestIdentifiedDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }
    }

    private static class TestUncompressableIdentifiedDataSerializable implements IdentifiedDataSerializable, Versioned {

        private int value;

        TestUncompressableIdentifiedDataSerializable() {
        }

        TestUncompressableIdentifiedDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public int getFactoryId() {
            return 2;
        }

        @Override
        public int getClassId() {
            return 311;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }
    }

    private static class TestNotInstantiableDataSerializable implements DataSerializable {

        TestNotInstantiableDataSerializable() {
            throw new HazelcastException("expected");
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
