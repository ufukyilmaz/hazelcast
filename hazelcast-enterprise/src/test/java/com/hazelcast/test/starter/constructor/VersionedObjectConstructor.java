package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = "com.hazelcast.collection.impl.queue.model.VersionedObject")
public class VersionedObjectConstructor extends AbstractStarterObjectConstructor {

    public VersionedObjectConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        // obtain reference to constructor NativeMemoryData(long address, int size)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Object.class, int.class);

        Object object = getFieldValueReflectively(delegate, "object");
        Integer version = getFieldValueReflectively(delegate, "version");
        Object[] args = new Object[]{object, version};

        return constructor.newInstance(args);
    }
}
