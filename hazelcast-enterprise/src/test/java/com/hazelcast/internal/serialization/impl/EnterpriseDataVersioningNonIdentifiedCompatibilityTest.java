package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Version;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.ClusterVersion;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

/**
 * Verifies the compatibility between the old and new serializers for the DataSerializable.
 * It checks two things:
 * - if the un-versioned serialization works between old-new, old-old, new-new serializers
 * - if the versioned serialization works between old-new & new-new serializers (when class annotated with @versioned or not)
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseDataVersioningNonIdentifiedCompatibilityTest {

    private static ClusterVersion V3_8 = new ClusterVersion(3, 8);
    private static Version SERIALIZATION_VERSION_3_8 = new Version(8);

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private EnterpriseSerializationService oldSS = ss(false);
    private EnterpriseSerializationService newSS = ss(true);

    @Test
    public void oldNew_Unversioned() {
        TestDataSerializable original = new TestDataSerializable(123);

        Data data = oldSS.toData(original);
        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Unversioned() {
        TestDataSerializable original = new TestDataSerializable(123);

        Data data = newSS.toData(original);
        TestDataSerializable deserialized = oldSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_Versioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        Data data = oldSS.toData(original);
        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Versioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        Data data = newSS.toData(original);

        // this can't work since the old code does not understand the versioned serialization
        expected.expect(HazelcastSerializationException.class);
        oldSS.toObject(data);
    }

    @Test
    public void newNew_Unversioned() {
        TestDataSerializable original = new TestDataSerializable(123);

        Data data = newSS.toData(original);
        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_Versioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        Data data = newSS.toData(original);
        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(SERIALIZATION_VERSION_3_8, original.serializationVersion);
        assertEquals(SERIALIZATION_VERSION_3_8, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = oldSS.toData(original);
        // we're just tricking the system since we're on one JVM and we can't have a class with the same name twice (one annotated and one not).
        // In the byte stream we're changing className from `TestDataSerializable` to `TestVersionedDataSerializable`.
        // so that the class annotated with @Versioned is deserialized using a identical class but without the @Versioned annotation.
        data = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data);

        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_UnversionedToVersioned_givingClassToDeserializeTo() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = oldSS.toData(original);

        TestVersionedDataSerializable deserialized = newSS.toObject(data, TestVersionedDataSerializable.class);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = newSS.toData(original);
        data = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data);

        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = oldSS.toData(original);
        data = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data);

        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = newSS.toData(original);
        data = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data);

        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(SERIALIZATION_VERSION_3_8, original.serializationVersion);
        assertEquals(SERIALIZATION_VERSION_3_8, deserialized.deserializationVersion);
    }

    private static Data adjustClassNameOfDSInData(Class oldClass, Class newClass, Data data) {
        String oldClassName = oldClass.getName();
        String newClassName = newClass.getName();

        byte[] dataBytes = data.toByteArray();
        byte[] adjustedBytes = new byte[dataBytes.length
                - oldClass.getName().length() + newClass.getName().length()];

        int i = 0;
        int dsDataOutputHeaderLengthInBytes = 9;
        for (int k = 0; k < dsDataOutputHeaderLengthInBytes; k++) {
            adjustedBytes[i++] = dataBytes[k];
        }

        Bits.writeInt(adjustedBytes, i, newClassName.length(), true);
        i += INT_SIZE_IN_BYTES;
        for (int k = 0; k < newClassName.length(); k++) {
            i += Bits.writeUtf8Char(adjustedBytes, i, newClassName.charAt(k));
        }
        for (int j = dsDataOutputHeaderLengthInBytes + INT_SIZE_IN_BYTES + oldClassName.length(); j < dataBytes.length; j++) {
            adjustedBytes[i++] = dataBytes[j];
        }
        return new HeapData(adjustedBytes);
    }

    private EnterpriseSerializationService ss(boolean rollingUpgradeEnabled) {
        EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1);
        if (rollingUpgradeEnabled) {
            builder.setClusterVersionAware(new TestClusterVersionAware()).setRollingUpgradeEnabled(rollingUpgradeEnabled);
        }
        return builder.build();
    }

    private static class TestDataSerializable implements DataSerializable {
        private int value;
        private Version deserializationVersion;
        private Version serializationVersion;

        public TestDataSerializable() {
        }

        public TestDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializationVersion = out.getVersion();
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializationVersion = in.getVersion();
            value = in.readInt();
        }
    }

    private static class TestVersionedDataSerializable implements DataSerializable, Versioned {
        private int value;
        private Version deserializationVersion;
        private Version serializationVersion;

        public TestVersionedDataSerializable() {
        }

        public TestVersionedDataSerializable(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializationVersion = out.getVersion();
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializationVersion = in.getVersion();
            value = in.readInt();
        }
    }

    private static class TestClusterVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public ClusterVersion getClusterVersion() {
            return V3_8;
        }
    }

}
