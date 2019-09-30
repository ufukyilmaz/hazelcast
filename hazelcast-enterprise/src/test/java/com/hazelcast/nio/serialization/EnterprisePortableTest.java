package com.hazelcast.nio.serialization;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static com.hazelcast.nio.serialization.PortableTest.ChildPortableObject;
import static com.hazelcast.nio.serialization.PortableTest.GrandParentPortableObject;
import static com.hazelcast.nio.serialization.PortableTest.ParentPortableObject;
import static com.hazelcast.nio.serialization.PortableTest.TestObject1;
import static com.hazelcast.nio.serialization.PortableTest.TestObject2;
import static com.hazelcast.nio.serialization.PortableTest.TestPortableFactory;
import static com.hazelcast.nio.serialization.PortableTest.createNamedPortableClassDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterprisePortableTest {

    static final int FACTORY_ID = TestSerializationConstants.PORTABLE_FACTORY_ID;

    @Test
    public void testBasics() {
        testBasics(ByteOrder.BIG_ENDIAN, false);
    }

    @Test
    public void testBasicsLittleEndian() {
        testBasics(ByteOrder.LITTLE_ENDIAN, false);
    }

    @Test
    public void testBasicsNativeOrder() {
        testBasics(ByteOrder.nativeOrder(), false);
    }

    @Test
    public void testBasicsNativeOrderUsingUnsafe() {
        testBasics(ByteOrder.nativeOrder(), true);
    }

    private void testBasics(ByteOrder order, boolean allowUnsafe) {
        InternalSerializationService serializationService = createSerializationService(1, order, allowUnsafe);
        InternalSerializationService serializationService2 = createSerializationService(2, order, allowUnsafe);
        Data data;

        NamedPortable[] nn = new NamedPortable[5];
        for (int i = 0; i < nn.length; i++) {
            nn[i] = new NamedPortable("named-portable-" + i, i);
        }

        NamedPortable np = nn[0];
        data = serializationService.toData(np);
        assertEquals(np, serializationService.toObject(data));
        assertEquals(np, serializationService2.toObject(data));

        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        data = serializationService.toData(inner);
        assertEquals(inner, serializationService.toObject(data));
        assertEquals(inner, serializationService2.toObject(data));

        MainPortable main = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        data = serializationService.toData(main);
        assertEquals(main, serializationService.toObject(data));
        assertEquals(main, serializationService2.toObject(data));
    }

    static EnterpriseSerializationService createSerializationService(int version) {
        return createSerializationService(version, ByteOrder.BIG_ENDIAN, false);
    }

    static EnterpriseSerializationService createSerializationService(int version, ByteOrder order, boolean allowUnsafe) {
        return new EnterpriseSerializationServiceBuilder()
                .setUseNativeByteOrder(false).setAllowUnsafe(allowUnsafe).setByteOrder(order).setPortableVersion(version)
                .addPortableFactory(FACTORY_ID, new TestPortableFactory()).build();
    }

    @Test
    public void testRawData() {
        InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId());
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(1));
        serializationService.getPortableContext().registerClassDefinition(builder.build());

        Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testRawDataWithoutRegistering() {
        InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));

        Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRawDataInvalidWrite() {
        InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new InvalidRawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId());
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(1));
        serializationService.getPortableContext().registerClassDefinition(builder.build());

        Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testRawDataInvalidRead() {
        InternalSerializationService serializationService = createSerializationService(1);
        RawDataPortable p = new InvalidRawDataPortable2(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));
        ClassDefinitionBuilder builder = new ClassDefinitionBuilder(p.getFactoryId(), p.getClassId());
        builder.addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(1));
        serializationService.getPortableContext().registerClassDefinition(builder.build());

        Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testClassDefinitionConfigWithErrors() throws Exception {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(FACTORY_ID, new TestPortableFactory());
        serializationConfig.setPortableVersion(1);
        serializationConfig.addClassDefinition(
                new ClassDefinitionBuilder(FACTORY_ID, TestSerializationConstants.RAW_DATA_PORTABLE).addLongField("l")
                        .addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(1)).build());

        try {
            new EnterpriseSerializationServiceBuilder().setConfig(serializationConfig).build();
            fail("Should throw HazelcastSerializationException!");
        } catch (HazelcastSerializationException ignored) {
        }

        new EnterpriseSerializationServiceBuilder().setConfig(serializationConfig).setCheckClassDefErrors(false).build();

        // -- OR --

        serializationConfig.setCheckClassDefErrors(false);
        new EnterpriseSerializationServiceBuilder().setConfig(serializationConfig).build();
    }

    @Test
    public void testClassDefinitionConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(FACTORY_ID, new TestPortableFactory());
        serializationConfig.setPortableVersion(1);
        serializationConfig
                .addClassDefinition(
                        new ClassDefinitionBuilder(FACTORY_ID, TestSerializationConstants.RAW_DATA_PORTABLE)
                                .addLongField("l").addCharArrayField("c").addPortableField("p", createNamedPortableClassDefinition(1)).build())
                .addClassDefinition(new ClassDefinitionBuilder(FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE).addUTFField("name")
                        .addIntField("myint").build());

        InternalSerializationService serializationService
                = new EnterpriseSerializationServiceBuilder().setConfig(serializationConfig).build();
        RawDataPortable p = new RawDataPortable(System.currentTimeMillis(), "test chars".toCharArray(),
                new NamedPortable("named portable", 34567),
                9876, "Testing raw portable", new ByteArrayDataSerializable("test bytes".getBytes()));

        Data data = serializationService.toData(p);
        assertEquals(p, serializationService.toObject(data));
    }

    @Test
    public void testPortableNestedInOthers() {
        InternalSerializationService serializationService = createSerializationService(1);
        Object o1 = new ComplexDataSerializable(new NamedPortable("test-portable", 137),
                new ByteArrayDataSerializable("test-data-serializable".getBytes()),
                new ByteArrayDataSerializable("test-data-serializable-2".getBytes()));

        Data data = serializationService.toData(o1);

        InternalSerializationService serializationService2 = createSerializationService(2);

        Object o2 = serializationService2.toObject(data);
        assertEquals(o1, o2);
    }

    //https://github.com/hazelcast/hazelcast/issues/1096
    @Test
    public void test_1096_ByteArrayContentSame() {
        InternalSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new TestPortableFactory()).build();

        assertRepeatedSerialisationGivesSameByteArrays(ss, new NamedPortable("issue-1096", 1096));

        assertRepeatedSerialisationGivesSameByteArrays(ss, new InnerPortable(new byte[3], new char[5], new short[2],
                new int[10], new long[7], new float[9], new double[1], new NamedPortable[]{new NamedPortable("issue-1096", 1096)}));

        assertRepeatedSerialisationGivesSameByteArrays(ss,
                new RawDataPortable(1096L, "issue-1096".toCharArray(), new NamedPortable("issue-1096", 1096), 1096,
                        "issue-1096", new ByteArrayDataSerializable(new byte[1])));
    }

    private static void assertRepeatedSerialisationGivesSameByteArrays(InternalSerializationService ss, Portable p) {
        Data data1 = ss.toData(p);
        for (int k = 0; k < 100; k++) {
            Data data2 = ss.toData(p);
            assertEquals(data1, data2);
        }
    }

    /**
     * Test for https://github.com/hazelcast/hazelcast/issues/2172
     */
    @Test
    public void test_issue2172_WritePortableArray() {
        InternalSerializationService ss = new EnterpriseSerializationServiceBuilder().setInitialOutputBufferSize(16).build();
        TestObject2[] testObject2s = new TestObject2[100];
        for (int i = 0; i < testObject2s.length; i++) {
            testObject2s[i] = new TestObject2();
        }

        TestObject1 testObject1 = new TestObject1(testObject2s);
        ss.toData(testObject1);
    }

    @Test
    public void testClassDefinitionLookupBigEndianHeapData() throws Exception {
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setByteOrder(ByteOrder.BIG_ENDIAN)
                .build();

        testClassDefinitionLookup(ss, DataType.HEAP);
    }

    @Test
    public void testClassDefinitionLookupLittleEndianHeapData() throws Exception {
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setByteOrder(ByteOrder.LITTLE_ENDIAN)
                .build();

        testClassDefinitionLookup(ss, DataType.HEAP);
    }

    @Test
    public void testClassDefinitionLookupNativeOrderHeapData() throws Exception {
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setUseNativeByteOrder(true)
                .build();

        testClassDefinitionLookup(ss, DataType.HEAP);
    }

    @Test
    public void testClassDefinitionLookupBigEndianOffHeapData() throws Exception {
        HazelcastMemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setByteOrder(ByteOrder.BIG_ENDIAN)
                .setMemoryManager(memoryManager)
                .build();

        testClassDefinitionLookup(ss, DataType.NATIVE);
        memoryManager.dispose();
    }

    @Test
    public void testClassDefinitionLookupLittleEndianOffHeapData() throws Exception {
        HazelcastMemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setByteOrder(ByteOrder.LITTLE_ENDIAN)
                .setMemoryManager(memoryManager)
                .build();

        testClassDefinitionLookup(ss, DataType.NATIVE);
        memoryManager.dispose();
    }

    @Test
    public void testClassDefinitionLookupNativeOrderOffHeapData() throws Exception {
        HazelcastMemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setUseNativeByteOrder(true)
                .setMemoryManager(memoryManager)
                .build();

        testClassDefinitionLookup(ss, DataType.NATIVE);
        memoryManager.dispose();
    }

    @Test
    public void testClassDefinitionLookupUnsafeNativeOrderOffHeapData() throws Exception {
        HazelcastMemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder()
                .setUseNativeByteOrder(true)
                .setAllowUnsafe(true)
                .setMemoryManager(memoryManager)
                .build();

        testClassDefinitionLookup(ss, DataType.NATIVE);
        memoryManager.dispose();
    }

    private void testClassDefinitionLookup(EnterpriseSerializationService ss, DataType dataType) throws Exception {
        NamedPortableV2 p = new NamedPortableV2("test-portable", 123456789, 1);
        Data data = ss.toData(p, dataType);

        PortableContext portableContext = ss.getPortableContext();
        ClassDefinition cd = portableContext.lookupClassDefinition(data);

        assertEquals(p.getFactoryId(), cd.getFactoryId());
        assertEquals(p.getClassId(), cd.getClassId());
        assertEquals(p.getClassVersion(), cd.getVersion());
    }

    @Test
    public void testSerializationService_createPortableReader() throws Exception {
        InternalSerializationService serializationService = new EnterpriseSerializationServiceBuilder().build();

        ChildPortableObject child = new ChildPortableObject(System.nanoTime());
        ParentPortableObject parent = new ParentPortableObject(System.currentTimeMillis(), child);
        GrandParentPortableObject grandParent = new GrandParentPortableObject(System.nanoTime(), parent);

        Data data = serializationService.toData(grandParent);
        PortableReader reader = serializationService.createPortableReader(data);

        assertEquals(grandParent.timestamp, reader.readLong("timestamp"));
        assertEquals(parent.timestamp, reader.readLong("child.timestamp"));
        assertEquals(child.timestamp, reader.readLong("child.child.timestamp"));
    }

    @Test
    public void testClassDefinition_getNestedField() throws Exception {
        InternalSerializationService serializationService = new EnterpriseSerializationServiceBuilder().build();
        PortableContext portableContext = serializationService.getPortableContext();

        ChildPortableObject child = new ChildPortableObject(System.nanoTime());
        ParentPortableObject parent = new ParentPortableObject(System.currentTimeMillis(), child);
        GrandParentPortableObject grandParent = new GrandParentPortableObject(System.nanoTime(), parent);

        Data data = serializationService.toData(grandParent);
        ClassDefinition classDefinition = portableContext.lookupClassDefinition(data);

        FieldDefinition fd = portableContext.getFieldDefinition(classDefinition, "child");
        assertNotNull(fd);
        assertEquals(FieldType.PORTABLE, fd.getType());

        fd = portableContext.getFieldDefinition(classDefinition, "child.child");
        assertNotNull(fd);
        assertEquals(FieldType.PORTABLE, fd.getType());

        fd = portableContext.getFieldDefinition(classDefinition, "child.child.timestamp");
        assertNotNull(fd);
        assertEquals(FieldType.LONG, fd.getType());
    }
}
