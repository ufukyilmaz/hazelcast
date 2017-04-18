package com.hazelcast.test.starter;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;

/**
 *
 */
public class MapEventConstructor extends AbstractStarterObjectConstructor {

    public MapEventConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Class<?> memberClass = starterClassLoader.loadClass("com.hazelcast.core.Member");
        Constructor<?> constructor = targetClass.getConstructor(Object.class, memberClass, Integer.TYPE, Integer.TYPE);

        Object source = getFieldValueReflectively(delegate, "source");
        Object member = getFieldValueReflectively(delegate, "member");
        Object entryEventType = getFieldValueReflectively(delegate, "entryEventType");
        Integer eventTypeId = (Integer) entryEventType.getClass().getMethod("getType").invoke(entryEventType);
        Object numberOfKeysAffected = getFieldValueReflectively(delegate, "numberOfEntriesAffected");

        Object[] args = new Object[] {source, member, eventTypeId.intValue(), numberOfKeysAffected};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }
}
