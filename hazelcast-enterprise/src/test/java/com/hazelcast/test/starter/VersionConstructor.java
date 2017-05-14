package com.hazelcast.test.starter;

import java.lang.reflect.Method;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

/**
 * Constructor for {@link com.hazelcast.version.Version} class proxies
 */
public class VersionConstructor extends AbstractStarterObjectConstructor {

    public VersionConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Class<?> versionClass = starterClassLoader.loadClass("com.hazelcast.version.Version");
        Method versionOf = versionClass.getDeclaredMethod("of", Integer.TYPE, Integer.TYPE);

        Byte major = (Byte) getFieldValueReflectively(delegate, "major");
        Byte minor = (Byte) getFieldValueReflectively(delegate, "minor");

        Object[] args = new Object[] {major.intValue(), minor.intValue()};

        return versionOf.invoke(null, args);
    }
}
