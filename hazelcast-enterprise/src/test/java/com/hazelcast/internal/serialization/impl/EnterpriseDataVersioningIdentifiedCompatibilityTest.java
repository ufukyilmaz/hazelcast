package com.hazelcast.internal.serialization.impl;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
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

import static com.hazelcast.internal.cluster.Versions.V3_8;
import static org.junit.Assert.assertEquals;

/**
 * Verifies the compatibility between the old and new serializers for the IdentifiedDataSerializable.
 * It checks two things:
 * - if the un-versioned serialization works between old-new, old-old, new-new serializers
 * - if the versioned serialization works between old-new & new-new serializers (when class annotated with @versioned or not)
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseDataVersioningIdentifiedCompatibilityTest {

    private static final Version CURRENT_VERSION = Version.of(BuildInfoProvider.getBuildInfo().getVersion());

    private static final DataSerializableFactory UNVERSIONED_FACTORY = new TestDataSerializableFactory();
    private static final DataSerializableFactory VERSIONED_FACTORY = new TestVersionedDataSerializableFactory();

    EnterpriseSerializationService oldUnversionedSS = ss(false, UNVERSIONED_FACTORY);
    EnterpriseSerializationService newUnversionedSS = ss(true, UNVERSIONED_FACTORY);

    EnterpriseSerializationService oldVersionedSS = ss(false, VERSIONED_FACTORY);
    EnterpriseSerializationService newVersionedSS = ss(true, VERSIONED_FACTORY);

    SerializationService ossUnversionedSS = ossSS(UNVERSIONED_FACTORY);
    SerializationService ossVersionedSS = ossSS(VERSIONED_FACTORY);

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void oldNew_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = oldUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = newUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = newUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = oldUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = newUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = ossUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void oldOld_Unversioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = oldUnversionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = oldUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
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
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = oldVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = ossVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void oldOld_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = oldVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_Versioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = newVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(V3_8, deserialized.deserializationVersion);
    }

    @Test
    public void oldNew_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = newVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = newUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = oldVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        Data data = newUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = ossVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(Version.UNKNOWN, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void oldOld_UnversionedToVersioned() {
        TestIdentifiedDataSerializable original = new TestIdentifiedDataSerializable(123);

        // versioned factory will be used as unversioned one by the "old" service
        Data data = oldUnversionedSS.toData(original);
        TestVersionedIdentifiedDataSerializable deserialized = oldVersionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
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
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(Version.UNKNOWN, deserialized.deserializationVersion);
    }

    @Test
    public void newOld_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = oldUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newOss_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = ossUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void oldOld_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        // versioned factory will be used as an unversioned one by the "old" service
        Data data = oldVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = oldUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(CURRENT_VERSION, original.serializationVersion);
        assertEquals(CURRENT_VERSION, deserialized.deserializationVersion);
    }

    @Test
    public void newNew_VersionedToUnversioned() {
        TestVersionedIdentifiedDataSerializable original = new TestVersionedIdentifiedDataSerializable(123);

        Data data = newVersionedSS.toData(original);
        TestIdentifiedDataSerializable deserialized = newUnversionedSS.toObject(data);

        assertEquals(original.value, deserialized.value);
        assertEquals(V3_8, original.serializationVersion);
        assertEquals(V3_8, deserialized.deserializationVersion);
    }

    private EnterpriseSerializationService ss(boolean versionedSerializationEnabled, DataSerializableFactory factory) {
        EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1);
        if (versionedSerializationEnabled) {
            builder.setClusterVersionAware(new TestClusterVersionAware()).setVersionedSerializationEnabled(versionedSerializationEnabled);
        }
        builder.addDataSerializableFactory(1, factory);
        return builder.build();
    }

    private SerializationService ossSS(DataSerializableFactory factory) {
        SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1);
        builder.addDataSerializableFactory(1, factory);
        return builder.build();
    }

    private static class TestIdentifiedDataSerializable implements IdentifiedDataSerializable {
        private int value;
        private Version deserializationVersion;
        private Version serializationVersion;

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

        TestVersionedIdentifiedDataSerializable() {
        }

        TestVersionedIdentifiedDataSerializable(int value) {
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
        public IdentifiedDataSerializable create(int typeId, Version version, Version wanProtocolVersion) {
            if (typeId == 1) {
                return new TestVersionedIdentifiedDataSerializable();
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
