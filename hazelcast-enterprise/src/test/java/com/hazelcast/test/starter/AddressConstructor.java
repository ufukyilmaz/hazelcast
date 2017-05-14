package com.hazelcast.test.starter;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

public class AddressConstructor extends AbstractStarterObjectConstructor {

    public AddressConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        // obtain reference to constructor Address(String host, int port)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(String.class, Integer.TYPE);

        Object host = getFieldValueReflectively(delegate, "host");
        Integer port = (Integer) getFieldValueReflectively(delegate, "port");
        Object[] args = new Object[] {host, port.intValue()};

        return constructor.newInstance(args);
    }
}
