package com.hazelcast.test.starter;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

public class DataAwareEntryEventConstructor extends AbstractStarterObjectConstructor {

    public DataAwareEntryEventConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        // locate required classes on target class loader
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Class<?> dataClass = starterClassLoader.loadClass("com.hazelcast.nio.serialization.Data");
        Class<?> memberClass = starterClassLoader.loadClass("com.hazelcast.core.Member");
        Class<?> serServiceClass = starterClassLoader.loadClass("com.hazelcast.spi.serialization.SerializationService");
        Constructor<?> constructor = targetClass.getConstructor(memberClass, Integer.TYPE, String.class, dataClass,
                dataClass, dataClass, dataClass, serServiceClass);

        Object serializationService = getFieldValueReflectively(delegate, "serializationService");
        Object source = getFieldValueReflectively(delegate, "source");
        Object member = getFieldValueReflectively(delegate, "member");
        Object entryEventType = getFieldValueReflectively(delegate, "entryEventType");
        Integer eventTypeId = (Integer) entryEventType.getClass().getMethod("getType").invoke(entryEventType);
        Object dataKey = getFieldValueReflectively(delegate, "dataKey");
        Object dataNewValue = getFieldValueReflectively(delegate, "dataNewValue");
        Object dataOldValue = getFieldValueReflectively(delegate, "dataOldValue");
        Object dataMergingValue = getFieldValueReflectively(delegate, "dataMergingValue");

        Object[] args = new Object[] {member, eventTypeId.intValue(), source,
                                      dataKey, dataNewValue,
                                      dataOldValue, dataMergingValue,
                                      serializationService};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }

}
