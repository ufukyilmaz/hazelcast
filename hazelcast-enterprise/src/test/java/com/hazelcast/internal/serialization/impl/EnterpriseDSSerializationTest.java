package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseDSSerializationTest {

    private static final Version V3_8 = Version.of("3.8");

    private boolean versionedSerializationEnabled;

    public EnterpriseDSSerializationTest(boolean versionedSerializationEnabled) {
        this.versionedSerializationEnabled = versionedSerializationEnabled;
    }

    private SerializationService ss;

    @Before
    public void init() {
        ss = new EnterpriseSerializationServiceBuilder()
                .setClusterVersionAware(new TestVersionAware())
                .setVersionedSerializationEnabled(versionedSerializationEnabled)
                .setVersion(InternalSerializationService.VERSION_1)
                .build();
    }

    @Test
    public void serializeAndDeserialize_DataSerializable() {
        DSPerson person = new DSPerson("James Bond");

        DSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }

    @Test
    public void serializeAndDeserialize_IdentifiedDataSerializable() {
        IDSPerson person = new IDSPerson("James Bond");
        SerializationService ss = new EnterpriseSerializationServiceBuilder()
                .addDataSerializableFactory(1, new IDSPersonFactory())
                .setClusterVersionAware(new TestVersionAware())
                .setVersion(InternalSerializationService.VERSION_1)
                .build();

        IDSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }

    @Test
    public void serializeAndDeserialize_IdentifiedDataSerializable_versionedFactory() {
        IDSPerson person = new IDSPerson("James Bond");
        SerializationService ss = new EnterpriseSerializationServiceBuilder()
                .addDataSerializableFactory(1, new IDSPersonFactoryVersioned())
                .setClusterVersionAware(new TestVersionAware())
                .setVersionedSerializationEnabled(versionedSerializationEnabled)
                .setVersion(InternalSerializationService.VERSION_1)
                .build();

        IDSPerson deserialized = ss.toObject(ss.toData(person));

        assertEquals(person.getClass(), deserialized.getClass());
        assertEquals(person.name, deserialized.name);
    }

    private static class DSPerson implements DataSerializable {

        private String name;

        DSPerson() {
        }

        DSPerson(String name) {
            this.name = name;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
        }
    }

    private static class IDSPerson implements IdentifiedDataSerializable {

        private String name;

        IDSPerson() {
        }

        IDSPerson(String name) {
            this.name = name;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }
    }

    private static class IDSPersonFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new IDSPerson();
        }
    }

    private class IDSPersonFactoryVersioned implements VersionedDataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (versionedSerializationEnabled) {
                throw new RuntimeException("Should not be used in versioned context");
            } else {
                return new IDSPerson();
            }
        }

        @Override
        public IdentifiedDataSerializable create(int typeId, Version version) {
            if (versionedSerializationEnabled) {
                return new IDSPerson();
            } else {
                throw new RuntimeException("Should not be used outside of versioned context");
            }
        }
    }

    private class TestVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return V3_8;
        }
    }

    @Parameterized.Parameters(name = "{index}: versionedSerializationEnabled = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }
}
