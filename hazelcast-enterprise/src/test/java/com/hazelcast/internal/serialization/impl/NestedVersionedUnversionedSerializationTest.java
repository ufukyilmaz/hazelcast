package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that as versioned/unversioned objects are read/written, Input/OutputObjectData.version is set
 * properly.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@Ignore
public class NestedVersionedUnversionedSerializationTest {

    private SerializationService serializationService;

    @Before
    public void setup() {
        TestVersionAware versionAware = new TestVersionAware();
        DataSerializableFactory testClassesFactory = new TestNestedClassesDataSerializableFactory();
        serializationService = new EnterpriseSerializationServiceBuilder()
                .setVersionedSerializationEnabled(true)
                .setClusterVersionAware(versionAware)
                .addDataSerializableFactory(1, testClassesFactory)
                .build();
    }

    @Test
    public void test_versionedDS_notVersionedDS() {
        ParentVersionedDataSerializable parent = new ParentVersionedDataSerializable(new ChildDataSerializable());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_versionedDS_notVersionedIDS() {
        ParentVersionedDataSerializable parent = new ParentVersionedDataSerializable(new ChildNotVersionedIDS());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_versionedIDS_notVersionedDS() {
        ParentVersionedIDS parent = new ParentVersionedIDS(new ChildDataSerializable());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_versionedIDS_notVersionedIDS() {
        ParentVersionedIDS parent = new ParentVersionedIDS(new ChildNotVersionedIDS());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_notVersionedDS_versionedIDS_notVersionedDS() {
        CompositeNotVersionedDS parent = new CompositeNotVersionedDS(
                new ParentVersionedIDS(new ChildNotVersionedIDS()),
                new ChildDataSerializable());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_notVersionedDS_versionedDS_notVersionedDS() {
        CompositeNotVersionedDS parent = new CompositeNotVersionedDS(
                new ParentVersionedDataSerializable(new ChildNotVersionedIDS()),
                new ChildDataSerializable());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_notVersionedDS_versionedIDS_notVersionedIDS() {
        CompositeNotVersionedDS parent = new CompositeNotVersionedDS(
                new ParentVersionedIDS(new ChildDataSerializable()),
                new ChildNotVersionedIDS());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    @Test
    public void test_notVersionedDS_versionedDS_notVersionedIDS() {
        CompositeNotVersionedDS parent = new CompositeNotVersionedDS(
                new ParentVersionedDataSerializable(new ChildDataSerializable()),
                new ChildNotVersionedIDS());
        Data data = serializationService.toData(parent);

        serializationService.toObject(data);
    }

    static class ParentVersionedDataSerializable implements DataSerializable, Versioned {

        private ChildDataSerializable child;

        ParentVersionedDataSerializable() {
        }

        ParentVersionedDataSerializable(ChildDataSerializable child) {
            this.child = child;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeObject(child);
            assertEquals(TestVersionAware.VERSION, out.getVersion());
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            child = in.readObject();
            assertEquals(TestVersionAware.VERSION, in.getVersion());
        }
    }

    static class ChildDataSerializable implements DataSerializable {

        ChildDataSerializable() {
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            assertTrue(out.getVersion().isUnknown());
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            assertTrue(in.getVersion().isUnknown());
        }
    }

    static class ParentVersionedIDS extends ParentVersionedDataSerializable implements IdentifiedDataSerializable {

        ParentVersionedIDS() {
        }

        ParentVersionedIDS(ChildDataSerializable child) {
            super(child);
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }
    }

    static class ChildNotVersionedIDS extends ChildDataSerializable implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }
    }

    static class CompositeNotVersionedDS implements DataSerializable {
        private ParentVersionedDataSerializable parent;
        private ChildDataSerializable child;

        CompositeNotVersionedDS() {
        }

        CompositeNotVersionedDS(ParentVersionedDataSerializable parent, ChildDataSerializable child) {
            this.parent = parent;
            this.child = child;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            assertEquals(Version.UNKNOWN, out.getVersion());
            out.writeObject(parent);
            out.writeObject(child);
            assertEquals(Version.UNKNOWN, out.getVersion());
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            assertEquals(Version.UNKNOWN, in.getVersion());
            parent = in.readObject();
            child = in.readObject();
            assertEquals(Version.UNKNOWN, in.getVersion());
        }
    }

    static class CompositeNotVersionedIDS extends CompositeNotVersionedDS implements IdentifiedDataSerializable {
        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 3;
        }
    }

    private static class TestNestedClassesDataSerializableFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case 1:
                    return new ParentVersionedIDS();
                case 2:
                    return new ChildNotVersionedIDS();
                case 3:
                    return new CompositeNotVersionedIDS();
                default:
                    throw new RuntimeException("Unsupported typeId: " + typeId);
            }
        }
    }

    private static class TestVersionAware implements EnterpriseClusterVersionAware {
        static final Version VERSION = Versions.V3_9;

        @Override
        public Version getClusterVersion() {
            return VERSION;
        }
    }
}
