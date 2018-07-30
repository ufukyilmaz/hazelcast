package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.internal.serialization.impl.NativeMemoryData"})
public class NativeMemoryDataConstructor extends AbstractStarterObjectConstructor {

    public NativeMemoryDataConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        // obtain reference to constructor NativeMemoryData(long address, int size)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Long.TYPE, Integer.TYPE);

        Long address = (Long) getFieldValueReflectively(delegate, "address");
        Integer size = (Integer) getFieldValueReflectively(delegate, "size");
        Object[] args = new Object[]{address, size};

        return constructor.newInstance(args);
    }
}
