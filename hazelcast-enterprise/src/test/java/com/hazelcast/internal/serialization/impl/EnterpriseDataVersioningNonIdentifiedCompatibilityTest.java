package com.hazelcast.internal.serialization.impl;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

/**
 * Verifies the compatibility between the old and new serializers for the DataSerializable.
 * It checks two things:
 * - if the un-versioned serialization works between old-new, old-old, new-new serializers
 * - if the versioned serialization works between old-new & new-new serializers (when class annotated with @versioned or not)
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseDataVersioningNonIdentifiedCompatibilityTest {

    private static final Version V3_8 = Version.of(3, 8);
    private static final Version CURRENT_VERSION = Version.of(BuildInfoProvider.getBuildInfo().getVersion());

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private EnterpriseSerializationService oldSS = ss(false);
    private EnterpriseSerializationService newSS = ss(true);

    private InternalSerializationService ossSS = ossSS();

    @Test
    public void oldNew_Unversioned() {
        TestDataSerializable original = new TestDataSerializable(123);

        Data data = oldSS.toData(original);
        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Unversioned() {
        TestDataSerializable original = new TestDataSerializable(123);

        Data data = newSS.toData(original);
        TestDataSerializable deserialized = oldSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_Unversioned() {
        TestDataSerializable original = new TestDataSerializable(123);

        Data data = newSS.toData(original);
        TestDataSerializable deserialized = ossSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_Versioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        Data data = oldSS.toData(original);
        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Versioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        Data data = newSS.toData(original);
        TestVersionedDataSerializable deserialized = oldSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_Versioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);

        Data data = newSS.toData(original);
        TestVersionedDataSerializable deserialized = ossSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
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
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(V3_8, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = oldSS.toData(original);
        // we're just tricking the system since we're on one JVM and we can't have a class with the same name twice (one annotated and one not).
        // In the byte stream we're changing className from `TestDataSerializable` to `TestVersionedDataSerializable`.
        // so that the class annotated with @Versioned is deserialized using a identical class but without the @Versioned annotation.
        data
                = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data, oldSS.getByteOrder());

        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = newSS.toData(original);
        // we're just tricking the system since we're on one JVM and we can't have a class with the same name twice (one annotated and one not).
        // In the byte stream we're changing className from `TestDataSerializable` to `TestVersionedDataSerializable`.
        // so that the class annotated with @Versioned is deserialized using a identical class but without the @Versioned annotation.
        data
                = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data, newSS.getByteOrder());

        TestVersionedDataSerializable deserialized = oldSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = newSS.toData(original);
        // we're just tricking the system since we're on one JVM and we can't have a class with the same name twice (one annotated and one not).
        // In the byte stream we're changing className from `TestDataSerializable` to `TestVersionedDataSerializable`.
        // so that the class annotated with @Versioned is deserialized using a identical class but without the @Versioned annotation.
        data
                = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data, newSS.getByteOrder());

        TestVersionedDataSerializable deserialized = ossSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void ossNew_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = ossSS.toData(original);
        // we're just tricking the system since we're on one JVM and we can't have a class with the same name twice (one annotated and one not).
        // In the byte stream we're changing className from `TestDataSerializable` to `TestVersionedDataSerializable`.
        // so that the class annotated with @Versioned is deserialized using a identical class but without the @Versioned annotation.
        data
                = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data, ossSS.getByteOrder());

        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_UnversionedToVersioned_givingClassToDeserializeTo() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = oldSS.toData(original);

        TestVersionedDataSerializable deserialized = newSS.toObject(data, TestVersionedDataSerializable.class);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void ossNew_UnversionedToVersioned_givingClassToDeserializeTo() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = ossSS.toData(original);

        TestVersionedDataSerializable deserialized = newSS.toObject(data, TestVersionedDataSerializable.class);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_UnversionedToVersioned_givingClassToDeserializeTo() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = newSS.toData(original);

        TestVersionedDataSerializable deserialized = oldSS.toObject(data, TestVersionedDataSerializable.class);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_UnversionedToVersioned_givingClassToDeserializeTo() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = newSS.toData(original);

        TestVersionedDataSerializable deserialized = ossSS.toObject(data, TestVersionedDataSerializable.class);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }


    @Test
    public void newNew_UnversionedToVersioned() {
        TestDataSerializable original = new TestDataSerializable(123);
        Data data = newSS.toData(original);
        data
                = adjustClassNameOfDSInData(TestDataSerializable.class, TestVersionedDataSerializable.class, data, newSS.getByteOrder());

        TestVersionedDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = oldSS.toData(original);
        data
                = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data, oldSS.getByteOrder());

        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void ossNew_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = ossSS.toData(original);
        data
                = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data, ossSS.getByteOrder());

        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = newSS.toData(original);
        data
                = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data, newSS.getByteOrder());

        TestDataSerializable deserialized = oldSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = newSS.toData(original);
        data
                = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data, newSS.getByteOrder());

        TestDataSerializable deserialized = ossSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_VersionedToUnversioned() {
        TestVersionedDataSerializable original = new TestVersionedDataSerializable(123);
        Data data = newSS.toData(original);
        data
                = adjustClassNameOfDSInData(TestVersionedDataSerializable.class, TestDataSerializable.class, data, newSS.getByteOrder());

        TestDataSerializable deserialized = newSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(V3_8, deserialized.deserializationVersion);
    }

    private static Data adjustClassNameOfDSInData(Class oldClass, Class newClass, Data data, ByteOrder byteOrder) {
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

        Bits.writeInt(adjustedBytes, i, newClassName.length(), byteOrder == ByteOrder.BIG_ENDIAN);
        i += INT_SIZE_IN_BYTES;
        for (int k = 0; k < newClassName.length(); k++) {
            i += Bits.writeUtf8Char(adjustedBytes, i, newClassName.charAt(k));
        }
        for (int j = dsDataOutputHeaderLengthInBytes + INT_SIZE_IN_BYTES + oldClassName.length(); j < dataBytes.length; j++) {
            adjustedBytes[i++] = dataBytes[j];
        }
        return new HeapData(adjustedBytes);
    }

    private EnterpriseSerializationService ss(boolean versionedSerializationEnabled) {
        EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1);
        if (versionedSerializationEnabled) {
            builder.setClusterVersionAware(new TestClusterVersionAware()).setVersionedSerializationEnabled(versionedSerializationEnabled);
        }
        return builder.build();
    }

    private InternalSerializationService ossSS() {
        return new DefaultSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1).build();
    }

    private static class TestDataSerializable implements DataSerializable {
        private int value;
        private Version deserializationVersion;
        private Version serializationVersion;

        TestDataSerializable() {
        }

        TestDataSerializable(int value) {
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

        TestVersionedDataSerializable() {
        }

        TestVersionedDataSerializable(int value) {
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
        public Version getClusterVersion() {
            return V3_8;
        }
    }

}
