package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.serialization.EnterprisePortableTest.createSerializationService;
import static com.hazelcast.nio.serialization.PortableClassVersionTest.createInnerPortableClassDefinition;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterprisePortableClassVersionTest {

    private static final int FACTORY_ID = EnterprisePortableTest.FACTORY_ID;

    @Test
    public void testDifferentClassVersions() {
        InternalSerializationService serializationService = new EnterpriseSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        InternalSerializationService serializationService2 = new EnterpriseSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        PortableClassVersionTest.testDifferentClassVersions(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersions() {
        InternalSerializationService serializationService = new EnterpriseSerializationServiceBuilder().setPortableVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        InternalSerializationService serializationService2 = new EnterpriseSerializationServiceBuilder().setPortableVersion(2)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        PortableClassVersionTest.testDifferentClassVersions(serializationService, serializationService2);
    }

    @Test
    public void testDifferentClassVersionsUsingDataWriteAndRead() throws Exception {
        InternalSerializationService serializationService = new EnterpriseSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        InternalSerializationService serializationService2 = new EnterpriseSerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        PortableClassVersionTest.testDifferentClassVersionsUsingDataWriteAndRead(serializationService,
                serializationService2);
    }

    @Test
    public void testDifferentClassAndServiceVersionsUsingDataWriteAndRead() throws Exception {
        InternalSerializationService serializationService = new EnterpriseSerializationServiceBuilder().setPortableVersion(1)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                }).build();

        InternalSerializationService serializationService2 = new EnterpriseSerializationServiceBuilder().setPortableVersion(2)
                .addPortableFactory(FACTORY_ID, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new NamedPortableV2();
                    }
                }).build();

        PortableClassVersionTest.testDifferentClassVersionsUsingDataWriteAndRead(serializationService,
                serializationService2);
    }

    @Test
    public void testPreDefinedDifferentVersionsWithInnerPortable() {
        InternalSerializationService serializationService = createSerializationService(1);
        serializationService.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(1));

        InternalSerializationService serializationService2 = createSerializationService(2);
        serializationService2.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(2));

        NamedPortable[] nn = new NamedPortable[1];
        nn[0] = new NamedPortable("name", 123);
        InnerPortable inner = new InnerPortable(new byte[]{0, 1, 2}, new char[]{'c', 'h', 'a', 'r'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, nn);

        MainPortable mainWithInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", inner);

        testPreDefinedDifferentVersions(serializationService, serializationService2, mainWithInner);
    }

    @Test
    public void testPreDefinedDifferentVersionsWithNullInnerPortable() {
        InternalSerializationService serializationService = createSerializationService(1);
        serializationService.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(1));

        InternalSerializationService serializationService2 = createSerializationService(2);
        serializationService2.getPortableContext().registerClassDefinition(createInnerPortableClassDefinition(2));

        MainPortable mainWithNullInner = new MainPortable((byte) 113, true, 'x', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main portable object created for testing!", null);

        testPreDefinedDifferentVersions(serializationService, serializationService2, mainWithNullInner);
    }

    private void testPreDefinedDifferentVersions(InternalSerializationService serializationService,
                                                 InternalSerializationService serializationService2, MainPortable mainPortable) {

        Data data = serializationService.toData(mainPortable);
        assertEquals(mainPortable, serializationService2.toObject(data));
    }
}
