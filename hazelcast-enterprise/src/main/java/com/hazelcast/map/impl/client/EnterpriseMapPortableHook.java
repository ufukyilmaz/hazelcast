package com.hazelcast.map.impl.client;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_PORTABLE_FACTORY_ID;

/**
 * Contains enterprise only extensions for client portable serialization.
 */
public class EnterpriseMapPortableHook implements PortableHook {

    /**
     * Factory id for map portable factory.
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_MAP_PORTABLE_FACTORY,
            ENTERPRISE_MAP_PORTABLE_FACTORY_ID);

    /**
     * Index of {@link AccumulatorInfo} constructor.
     */
    public static final int CREATE_ACCUMULATOR_INFO = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new EnterpriseMapPortableFactory();
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }

    /**
     * Implements {@link PortableFactory} for this {@link PortableHook}
     */
    private static class EnterpriseMapPortableFactory implements PortableFactory {

        /**
         * Creates a Portable instance using given class id
         *
         * @param classId portable class id
         * @return portable instance or null if class id is not known by this factory
         */
        @Override
        public Portable create(int classId) {
            if (classId == CREATE_ACCUMULATOR_INFO) {
                return new AccumulatorInfo();
            }
            throw new IndexOutOfBoundsException(getExceptionMessage(classId));
        }

        private static String getExceptionMessage(int classId) {
            return "No registered constructor exists with class id: " + classId;
        }
    }

}

