package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Version;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
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

import static org.junit.Assert.assertEquals;

/**
 * Verifies the compatibility between the old and new serializers for the IdentifiedDataSerializable.
 * It checks two things:
 * - if the un-versioned serialization works between old-new, old-old, new-new serializers
 * - if the versioned serialization works between old-new & new-new serializers (when class annotated with @versioned or not)
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseDataVersioningIdentifiedCompatibilityTest {

    private static ClusterVersion V3_8 = new ClusterVersion(3, 8);
    private static Version SERIALIZATION_VERSION_3_8 = new Version(8);

    private static final DataSerializableFactory UNVERSIONED_FACTORY = new TestDataSerializableFactory();
    private static final DataSerializableFactory VERSIONED_FACTORY = new TestVersionedDataSerializableFactory();

    EnterpriseSerializationService oldUnversionedSS = ss(false, UNVERSIONED_FACTORY);
    EnterpriseSerializationService newUnversionedSS = ss(true, UNVERSIONED_FACTORY);

    EnterpriseSerializationService oldVersionedSS = ss(false, VERSIONED_FACTORY);
    EnterpriseSerializationService newVersionedSS = ss(true, VERSIONED_FACTORY);

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void oldNew_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = oldUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = newUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        // it can't work due to the factorId & classId compression
        expected.expect(HazelcastSerializationException.class);

        Data data = newUnversionedSS.toData(original);
        oldUnversionedSS.toObject(data);
    }

    @Test
    public void oldOld_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = oldUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = oldUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = newUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = newUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = newVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // it can't work due to the factorId & classId compression and version byte
        expected.expect(HazelcastSerializationException.class);

        Data data = newVersionedSS.toData(original);
        oldVersionedSS.toObject(data);
    }

    @Test
    public void oldOld_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = oldVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = newVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(SERIALIZATION_VERSION_3_8, original.serializationVersion);
        assertEquals(SERIALIZATION_VERSION_3_8, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = newVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        // it can't work due to the factorId & classId compression and version byte
        expected.expect(HazelcastSerializationException.class);

        Data data = newUnversionedSS.toData(original);
        oldVersionedSS.toObject(data);
    }

    @Test
    public void oldOld_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = oldVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = newUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = newVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // versioned factory will be used as an unversioned one by the "old" service
        Data data = oldVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = newUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // it can't work due to the factorId & classId compression and version byte
        expected.expect(HazelcastSerializationException.class);

        Data data = newVersionedSS.toData(original);
        oldUnversionedSS.toObject(data);
    }

    @Test
    public void oldOld_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // versioned factory will be used as an unversioned one by the "old" service
        Data data = oldVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = oldUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = newUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(SERIALIZATION_VERSION_3_8, original.serializationVersion);
        assertEquals(SERIALIZATION_VERSION_3_8, deserialized.deserializationVersion);
    }

    private EnterpriseSerializationService ss(boolean rollingUpgradeEnabled, DataSerializableFactory factory) {
        EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1);
        if (rollingUpgradeEnabled) {
            builder.setClusterVersionAware(new TestClusterVersionAware()).setRollingUpgradeEnabled(rollingUpgradeEnabled);
        }
        builder.addDataSerializableFactory(1, factory);
        return builder.build();
    }

    private static class TestIdentifiedDataSerializable implements IdentifiedDataSerializable {
        private int value;
        private Version deserializationVersion;
        private Version serializationVersion;

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
            serializationVersion = out.getVersion();
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializationVersion = in.getVersion();
            value = in.readInt();
        }
    }

    private static class TestVersionedIdentifiedDataSerializable implements IdentifiedDataSerializable, Versioned {
        private int value;
        private Version deserializationVersion;
        private Version serializationVersion;

        public TestVersionedIdentifiedDataSerializable() {
        }

        public TestVersionedIdentifiedDataSerializable(int value) {
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
            serializationVersion = out.getVersion();
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializationVersion = in.getVersion();
            value = in.readInt();
        }
    }

    private static class TestDataSerializableFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == 1) {
                return new TestIdentifiedDataSerializable();
            }
            throw new RuntimeException("Unsupported typeId");
        }
    }

    private static class TestVersionedDataSerializableFactory implements VersionedDataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == 1) {
                return new TestVersionedIdentifiedDataSerializable();
            }
            throw new RuntimeException("Unsupported typeId");
        }

        @Override
        public IdentifiedDataSerializable create(int typeId, Version version) {
            if (typeId == 1) {
                return new TestVersionedIdentifiedDataSerializable();
            }
            throw new RuntimeException("Unsupported typeId");
        }
    }

    private static class TestClusterVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public ClusterVersion getClusterVersion() {
            return V3_8;
        }
    }

}
