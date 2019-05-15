package com.hazelcast.nio.serialization;

import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.executor.impl.operations.CancellationOperation;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionAware;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationConcurrencyTest.Person;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category(QuickTest.class)
public class EnterpriseSerializationTest extends HazelcastTestSupport {

    private static final Version V3_8 = Version.of("3.8");
    private static final Version CURRENT_VERSION = Version.of(BuildInfoProvider.getBuildInfo().getVersion());

    private boolean versionedSerializationEnabled;

    public EnterpriseSerializationTest(boolean versionedSerializationEnabled) {
        this.versionedSerializationEnabled = versionedSerializationEnabled;
    }

    @Test
    public void testGlobalSerializer() {
        SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
                new GlobalSerializerConfig().setImplementation(new StreamSerializer<DummyValue>() {
                    @Override
                    public void write(ObjectDataOutput out, DummyValue v) throws IOException {
                        out.writeUTF(v.s);
                        out.writeInt(v.k);
                    }

                    @Override
                    public DummyValue read(ObjectDataInput in) throws IOException {
                        return new DummyValue(in.readUTF(), in.readInt());
                    }

                    @Override
                    public int getTypeId() {
                        return 123;
                    }

                    @Override
                    public void destroy() {
                    }
                }));

        InternalSerializationService ss1 = builder().setConfig(serializationConfig)
                .setClusterVersionAware(new TestVersionAware()).build();
        DummyValue value = new DummyValue("test", 111);
        Data data = ss1.toData(value);
        assertNotNull(data);

        InternalSerializationService ss2 = builder().setConfig(serializationConfig)
                .setClusterVersionAware(new TestVersionAware()).build();
        Object o = ss2.toObject(data);
        assertEquals(value, o);
    }

    @Test
    public void test_callId_on_correct_stream_position() throws Exception {
        InternalSerializationService serializationService = builder()
                .setClusterVersionAware(new TestVersionAware()).build();
        CancellationOperation operation = new CancellationOperation(newUnsecureUuidString(), true);
        operation.setCallerUuid(newUnsecureUuidString());
        OperationAccessor.setCallId(operation, 12345);

        Data data = serializationService.toData(operation);
        long callId = ((SerializationServiceV1) serializationService).initDataSerializableInputAndSkipTheHeader(data).readLong();

        assertEquals(12345, callId);
    }

    private static class DummyValue {
        String s;
        int k;

        private DummyValue(String s, int k) {
            this.s = s;
            this.k = k;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DummyValue that = (DummyValue) o;
            if (k != that.k) {
                return false;
            }
            return s != null ? s.equals(that.s) : that.s == null;
        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + k;
            return result;
        }
    }

    @Test
    public void testEmptyData() {
        SerializationConfig serializationConfig = new SerializationConfig().addSerializerConfig(
                new SerializerConfig().setTypeClass(SingletonValue.class)
                        .setImplementation(new StreamSerializer<SingletonValue>() {
                            @Override
                            public void write(ObjectDataOutput out, SingletonValue v) {
                            }

                            @Override
                            public SingletonValue read(ObjectDataInput in) {
                                return new SingletonValue();
                            }

                            @Override
                            public int getTypeId() {
                                return 123;
                            }

                            @Override
                            public void destroy() {
                            }
                        }));

        InternalSerializationService ss1 = builder().setConfig(serializationConfig)
                .setClusterVersionAware(new TestVersionAware()).build();
        Data data = ss1.toData(new SingletonValue());
        assertNotNull(data);

        InternalSerializationService ss2 = builder().setConfig(serializationConfig)
                .setClusterVersionAware(new TestVersionAware()).build();
        Object o = ss2.toObject(data);
        assertEquals(new SingletonValue(), o);
    }

    private static class SingletonValue {
        public boolean equals(Object obj) {
            return obj instanceof SingletonValue;
        }
    }

    @Test
    public void testNullData() {
        Data data = new HeapData();
        InternalSerializationService ss = builder()
                .setClusterVersionAware(new TestVersionAware()).build();
        assertNull(ss.toObject(data));
    }

    /**
     * issue #1265
     */
    @Test
    public void testSharedJavaSerialization() {
        InternalSerializationService ss = builder().setEnableSharedObject(true)
                .setClusterVersionAware(new TestVersionAware()).build();
        Data data = ss.toData(new Foo());
        Foo foo = ss.toObject(data);

        assertSame("Objects are not identical!", foo, foo.getBar().getFoo());
    }

    @Test
    public void testLinkedListSerialization() {
        InternalSerializationService ss = builder()
                .setClusterVersionAware(new TestVersionAware()).build();
        LinkedList<Person> linkedList = new LinkedList<Person>();
        linkedList.add(new Person(35, 180, 100, "Orhan", null));
        linkedList.add(new Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(linkedList);
        LinkedList deserialized = ss.toObject(data);
        assertEquals("Objects are not identical!", linkedList, deserialized);
    }

    @Test
    public void testArrayListSerialization() {
        InternalSerializationService ss = builder()
                .setClusterVersionAware(new TestVersionAware()).build();
        ArrayList<Person> arrayList = new ArrayList<Person>();
        arrayList.add(new Person(35, 180, 100, "Orhan", null));
        arrayList.add(new Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(arrayList);
        ArrayList deserialized = ss.toObject(data);
        assertEquals("Objects are not identical!", arrayList, deserialized);
    }

    @Test
    public void testArraySerialization() {
        InternalSerializationService ss = builder()
                .setClusterVersionAware(new TestVersionAware()).build();
        byte[] array = new byte[1024];
        new Random().nextBytes(array);
        Data data = ss.toData(array);
        byte[] deserialized = ss.toObject(data);
        assertArrayEquals(array, deserialized);
    }

    @Test
    public void testPartitionHash() {
        HazelcastMemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        PartitioningStrategy partitionStrategy = new PartitioningStrategy() {
            @Override
            public Object getPartitionKey(Object key) {
                return key.hashCode();
            }
        };

        EnterpriseSerializationService ss = builder()
                .setAllowUnsafe(true).setUseNativeByteOrder(true)
                .setMemoryManager(memoryManager)
                .setClusterVersionAware(new TestVersionAware()).build();

        String obj = String.valueOf(System.nanoTime());
        Data partitionKey = ss.toData(obj.hashCode(), DataType.NATIVE);
        assertFalse(partitionKey.hasPartitionHash());

        Data data = ss.toData(obj, DataType.HEAP, partitionStrategy);
        assertTrue(data.hasPartitionHash());
        assertNotEquals(data.hashCode(), data.getPartitionHash());
        assertEquals(partitionKey.hashCode(), data.getPartitionHash());

        Data data2 = ss.toData(obj, DataType.NATIVE, partitionStrategy);
        assertTrue(data2.hasPartitionHash());
        assertNotEquals(data2.hashCode(), data2.getPartitionHash());
        assertEquals(partitionKey.hashCode(), data2.getPartitionHash());

        ss.disposeData(partitionKey);
        ss.disposeData(data);
        ss.disposeData(data2);
        memoryManager.dispose();
    }

    /**
     * issue #1265
     */
    @Test
    public void testUnsharedJavaSerialization() {
        InternalSerializationService ss = builder().setEnableSharedObject(false)
                .setClusterVersionAware(new TestVersionAware()).build();
        Data data = ss.toData(new Foo());
        Foo foo = ss.toObject(data);

        assertNotSame("Objects should not be identical!", foo, foo.getBar().getFoo());
    }

    private static class Foo implements Serializable {

        public Bar bar;

        Foo() {
            this.bar = new Bar();
        }

        public Bar getBar() {
            return bar;
        }

        private class Bar implements Serializable {
            public Foo getFoo() {
                return Foo.this;
            }
        }
    }

    @Test
    public void testMemberLeftException_usingMemberImpl() throws Exception {
        String uuid = newUnsecureUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new MemberImpl.Builder(new Address(host, port))
                .version(MemberVersion.UNKNOWN)
                .uuid(uuid)
                .build();

        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_usingSimpleMember() throws Exception {
        String uuid = newUnsecureUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new SimpleMemberImpl(MemberVersion.UNKNOWN, uuid, new InetSocketAddress(host, port));
        testMemberLeftException(uuid, host, port, member);
    }

    private void testMemberLeftException(String uuid, String host, int port, Member member) throws Exception {

        MemberLeftException exception = new MemberLeftException(member);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bout);
        out.writeObject(exception);

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream in = new ObjectInputStream(bin);
        MemberLeftException exception2 = (MemberLeftException) in.readObject();
        MemberImpl member2 = (MemberImpl) exception2.getMember();

        assertEquals(uuid, member2.getUuid());
        assertEquals(host, member2.getAddress().getHost());
        assertEquals(port, member2.getAddress().getPort());
    }

    @Test(expected = NullPointerException.class)
    public void serializerNeedsClusterAware_fail() {
        new EnterpriseSerializationServiceBuilder()
                .setVersionedSerializationEnabled(true)
                .setVersion(InternalSerializationService.VERSION_1)
                .build();
    }

    @Test
    public void serializerNeedsClusterAware_success() {
        new EnterpriseSerializationServiceBuilder()
                .setVersionedSerializationEnabled(true)
                .setVersion(InternalSerializationService.VERSION_1)
                .setClusterVersionAware(new EnterpriseClusterVersionAware() {
                    @Override
                    public Version getClusterVersion() {
                        return V3_8;
                    }
                })
                .build();
    }

    @Test
    public void testNotVersionedDataSerializable_outputHasUnknownVersion() {
        TestVersionAware testVersionAware = new TestVersionAware();
        InternalSerializationService serializationService = builder().setClusterVersionAware(testVersionAware).build();
        SerializationV1DataSerializable object = new SerializationV1DataSerializable();
        serializationService.toData(object);
        Version expected = expectedVersion(false);
        assertEquals("ObjectDataOutput.getVersion should be " + expected, expected,
                object.getVersion());
    }

    @Test
    public void testNotVersionedDataSerializable_inputHasUnknownVersion() {
        TestVersionAware testVersionAware = new TestVersionAware();
        InternalSerializationService serializationService = builder().setClusterVersionAware(testVersionAware).build();
        SerializationV1DataSerializable object = new SerializationV1DataSerializable();
        SerializationV1DataSerializable otherObject = serializationService.toObject(serializationService.toData(object));
        Version expected = expectedVersion(false);
        assertEquals("ObjectDataInput.getVersion should be " + expected, expected,
                otherObject.getVersion());
    }

    @Test
    public void testVersionedDataSerializable_outputHasClusterVersion_whenVersionedSerializationEnabled() {
        TestVersionAware testVersionAware = new TestVersionAware();
        InternalSerializationService serializationService = builder().setClusterVersionAware(testVersionAware).build();
        VersionedDataSerializable object = new VersionedDataSerializable();
        serializationService.toData(object);
        assertEquals(expectedVersion(true), object.getVersion());
    }

    @Test
    public void testVersionedDataSerializable_inputHasClusterVersion_whenVersionedSerializationEnabled() {
        TestVersionAware testVersionAware = new TestVersionAware();
        InternalSerializationService serializationService = builder().setClusterVersionAware(testVersionAware).build();
        VersionedDataSerializable object = new VersionedDataSerializable();
        VersionedDataSerializable otherObject = serializationService.toObject(serializationService.toData(object));
        assertEquals(expectedVersion(true), otherObject.getVersion());
    }

    // when versioned serialization is enabled:
    //  if DataSerializable is not versioned -> expect UNKNOWN
    //  if DataSerializable is versioned -> expect version of VersionAware in serialization service
    // when versioned serialization is disabled
    //  expect current cluster version anyway as in this case the OSS DataSerializableSerializer is used,
    //  which is not version-aware)
    private Version expectedVersion(boolean versionedSerializable) {
        if (versionedSerializationEnabled) {
            return versionedSerializable ? V3_8 : Version.UNKNOWN;
        } else {
            return CURRENT_VERSION;
        }
    }

    @Parameterized.Parameters(name = "{index}: versionedSerializationEnabled = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }

    public EnterpriseSerializationServiceBuilder builder() {
        return new EnterpriseSerializationServiceBuilder().setVersionedSerializationEnabled(versionedSerializationEnabled);
    }

    private static class TestVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return V3_8;
        }
    }
}
