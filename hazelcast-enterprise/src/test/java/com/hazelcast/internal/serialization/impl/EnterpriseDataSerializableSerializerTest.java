package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.createHeader;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseDataSerializableSerializerTest {

    private static Version V3_8 = Version.of(3, 8, 0);

    private StreamSerializer<DataSerializable> enterpriseSerializer;

    @Before
    public void initSerializer() {
        Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();
        factories.put(1, new TestDataSerializableFactory());

        enterpriseSerializer = new EnterpriseDataSerializableSerializer(factories, this.getClass().getClassLoader(),
                new TestClusterVersionAware());
    }

    @Test
    public void byteFormat_read_DS_unversioned() throws IOException {
        TestDataSerializable original = new TestDataSerializable(123);
        VersionedObjectDataOutput output = mock(VersionedObjectDataOutput.class);

        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).setVersion(Version.UNKNOWN);
        calls.verify(output).writeByte(createHeader(false, false, false));
        calls.verify(output).writeUTF(TestDataSerializable.class.getName());
        calls.verify(output).writeInt(original.value);
        verifyNoMoreInteractions(output);
    }

    @Test
    public void byteFormat_DS_versioned() throws IOException {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        VersionedObjectDataOutput output = mock(VersionedObjectDataOutput.class);

        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).setVersion(V3_8);
        calls.verify(output).writeByte(createHeader(false, true, false));
        calls.verify(output).writeUTF(TestVersionedDataSerializable.class.getName());
        calls.verify(output).writeInt(original.value);
        // verifyNoMoreInteractions(output); // mockito calls a hash method on the proxy
    }

    @Test
    public void byetFormat_IDS_unversioned_compressed() throws IOException {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);
        VersionedObjectDataOutput output = mock(VersionedObjectDataOutput.class);

        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).setVersion(Version.UNKNOWN);
        calls.verify(output).writeByte(createHeader(true, false, true));
        calls.verify(output, times(2)).writeByte(original.getFactoryId());
        calls.verify(output).writeInt(original.value);
        verifyNoMoreInteractions(output);
    }

    @Test
    public void byteFormat_IDS_versioned_uncompressed() throws IOException {
        TestUncompressableIdentifiedDataSerializable original = new TestUncompressableIdentifiedDataSerializable(123);
        VersionedObjectDataOutput output = mock(VersionedObjectDataOutput.class);

        enterpriseSerializer.write(output, original);

        InOrder calls = inOrder(output);
        calls.verify(output).setVersion(V3_8);
        calls.verify(output).writeByte(createHeader(true, true, false));
        calls.verify(output).writeInt(original.getFactoryId());
        calls.verify(output).writeInt(original.getId());
        calls.verify(output).writeByte(8); // V3_8 minor byte
        calls.verify(output).writeInt(original.value);
        verifyNoMoreInteractions(output);
    }

    private static class TestDataSerializable implements DataSerializable {
        private int value;

        public TestDataSerializable() {
        }

        public TestDataSerializable(int value) {
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

        public TestVersionedDataSerializable() {
        }

        public TestVersionedDataSerializable(int value) {
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

        public TestIdentifiedDataSerializable() {
        }

        public TestIdentifiedDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getId() {
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

        public TestUncompressableIdentifiedDataSerializable() {
        }

        public TestUncompressableIdentifiedDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getId() {
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

    private static class TestDataSerializableFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == 1) {
                return new TestIdentifiedDataSerializable();
            }
            if (typeId == 311) {
                return new TestUncompressableIdentifiedDataSerializable();
            }
            throw new RuntimeException("Unsupported typeId");
        }
    }

    private static class TestClusterVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return V3_8;
        }
    }

}
