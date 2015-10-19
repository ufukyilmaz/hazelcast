package com.hazelcast.map.impl.client;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MutableInteger;

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
     * Array index variable for constructors.
     */
    public static final MutableInteger CONSTRUCTOR_ARRAY_INDEX = new MutableInteger();

    /**
     * Index of {@link PublisherCreateRequest} constructor.
     */
    public static final int CREATE_PUBLISHER_REQUEST = ++CONSTRUCTOR_ARRAY_INDEX.value;

    /**
     * Index of {@link AccumulatorInfo} constructor.
     */
    public static final int CREATE_ACCUMULATOR_INFO = ++CONSTRUCTOR_ARRAY_INDEX.value;

    /**
     * Index of {@link MapAddListenerAdapterRequest} constructor.
     */
    public static final int ADD_LISTENER_ADAPTER = ++CONSTRUCTOR_ARRAY_INDEX.value;

    /**
     * Index of {@link MadePublishableRequest} constructor.
     */
    public static final int MADE_PUBLISHABLE_REQUEST = ++CONSTRUCTOR_ARRAY_INDEX.value;

    /**
     * Index of {@link SetReadCursorRequest} constructor.
     */
    public static final int SET_READ_CURSOR_REQUEST = ++CONSTRUCTOR_ARRAY_INDEX.value;

    /**
     * Index of {@link DestroyQueryCacheRequest} constructor.
     */
    public static final int REMOVE_PUBLISHER_REQUEST = ++CONSTRUCTOR_ARRAY_INDEX.value;


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

        private final ConstructorFunction<Integer, Portable>[] constructors
                = new ConstructorFunction[CONSTRUCTOR_ARRAY_INDEX.value + 1];

        {
            constructors[CREATE_PUBLISHER_REQUEST] = newPublisherConstructor();
            constructors[MADE_PUBLISHABLE_REQUEST] = newMadePublishableRequestConstructor();
            constructors[CREATE_ACCUMULATOR_INFO] = newAccumulatorInfoConstructor();
            constructors[ADD_LISTENER_ADAPTER] = newMapListenerAdapterRequest();
            constructors[SET_READ_CURSOR_REQUEST] = newAccumulatorSetReadCursorRequestConstructor();
            constructors[REMOVE_PUBLISHER_REQUEST] = newRemovePublisherRequestConstructor();
        }

        private ConstructorFunction<Integer, Portable> newMapListenerAdapterRequest() {
            return new ConstructorFunction<Integer, Portable>() {
                @Override
                public Portable createNew(Integer arg) {
                    return new MapAddListenerAdapterRequest();
                }
            };
        }

        private ConstructorFunction<Integer, Portable> newPublisherConstructor() {
            return new ConstructorFunction<Integer, Portable>() {
                @Override
                public Portable createNew(Integer arg) {
                    return new PublisherCreateRequest();
                }
            };
        }

        private ConstructorFunction<Integer, Portable> newMadePublishableRequestConstructor() {
            return new ConstructorFunction<Integer, Portable>() {
                @Override
                public Portable createNew(Integer arg) {
                    return new MadePublishableRequest();
                }
            };
        }

        private ConstructorFunction<Integer, Portable> newAccumulatorSetReadCursorRequestConstructor() {
            return new ConstructorFunction<Integer, Portable>() {
                @Override
                public Portable createNew(Integer arg) {
                    return new SetReadCursorRequest();
                }
            };
        }

        private ConstructorFunction<Integer, Portable> newAccumulatorInfoConstructor() {
            return new ConstructorFunction<Integer, Portable>() {
                @Override
                public Portable createNew(Integer arg) {
                    return new AccumulatorInfo();
                }
            };
        }

        private ConstructorFunction<Integer, Portable> newRemovePublisherRequestConstructor() {
            return new ConstructorFunction<Integer, Portable>() {
                @Override
                public Portable createNew(Integer arg) {
                    return new DestroyQueryCacheRequest();
                }
            };
        }


        /**
         * Creates a Portable instance using given class id
         *
         * @param classId portable class id
         * @return portable instance or null if class id is not known by this factory
         */
        @Override
        public Portable create(int classId) {
            checkRange(classId);

            ConstructorFunction<Integer, Portable> constructor = constructors[classId];
            if (constructor == null) {
                throw new IllegalArgumentException(getExceptionMessage(classId));
            }
            return constructor.createNew(classId);
        }

        private static void checkRange(int classId) {
            int index = CONSTRUCTOR_ARRAY_INDEX.value;
            if (classId > index || index < 0) {
                throw new IndexOutOfBoundsException(getExceptionMessage(classId));
            }
        }


        private static String getExceptionMessage(int classId) {
            return "No registered constructor exists with class id: " + classId;
        }
    }

}

