package com.hazelcast.test.starter;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

/**
 *
 */
public class LifecycleEventConstructor extends AbstractStarterObjectConstructor {

    public LifecycleEventConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Class<?> stateClass = starterClassLoader.loadClass("com.hazelcast.core.LifecycleEvent$LifecycleState");
        Constructor<?> constructor = targetClass.getDeclaredConstructor(stateClass);

        Object state = getFieldValueReflectively(delegate, "state");
        Object[] args = new Object[] {state};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }
}
