package com.hazelcast.cache.hidensity.client;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * @author enesakar 2/11/14
 */
public class CachePortableHook implements PortableHook {

    /**
     * Id of "Cache Portable Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_PORTABLE_FACTORY, -25);
    /**
     * Id of "ADD_INVALIDATION_LISTENER" operation
     */
    public static final int ADD_INVALIDATION_LISTENER = 1;
    /**
     * Id of "INVALIDATION_MESSAGE" operation
     */
    public static final int INVALIDATION_MESSAGE = 2;
    /**
     * Id of "REMOVE_INVALIDATION_LISTENER" operation
     */
    public static final int REMOVE_INVALIDATION_LISTENER = 3;

    private static final int PORTABLE_COUNT = 3;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable>[] constructors =
                    new ConstructorFunction[PORTABLE_COUNT + 1];

            {
                constructors[ADD_INVALIDATION_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheAddInvalidationListenerRequest();
                    }
                };
                constructors[INVALIDATION_MESSAGE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheInvalidationMessage();
                    }
                };
                constructors[REMOVE_INVALIDATION_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheRemoveInvalidationListenerRequest();
                    }
                };
            }

            public Portable create(int classId) {
                if (constructors[classId] == null) {
                    throw new IllegalArgumentException("No registered constructor with class id:" + classId);
                }
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
