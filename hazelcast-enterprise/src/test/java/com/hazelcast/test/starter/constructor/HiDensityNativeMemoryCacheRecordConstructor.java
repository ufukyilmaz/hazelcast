package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.cache.impl.hidensity.nativememory.HiDensityNativeMemoryCacheRecord"})
public class HiDensityNativeMemoryCacheRecordConstructor extends AbstractStarterObjectConstructor {

    public HiDensityNativeMemoryCacheRecordConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> accessorClass = classloader.loadClass("com.hazelcast.internal.hidensity.HiDensityRecordAccessor");
        // obtain reference to constructor HiDensityNativeMemoryCacheRecord(
        //     HiDensityRecordAccessor<HiDensityNativeMemoryCacheRecord> recordAccessor, long address)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(accessorClass, Long.TYPE);

        Object recordAccessor = getFieldValueReflectively(delegate, "recordAccessor");
        Long address = (Long) getFieldValueReflectively(delegate, "address");
        Object[] args = new Object[]{recordAccessor, address};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, classloader);
        return constructor.newInstance(proxiedArgs);
    }
}
