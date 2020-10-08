package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.test.starter.HazelcastProxyFactory;
import com.hazelcast.test.starter.ProxyInvocationHandler;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Compatibility test for IAtomicLong.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicLongCompatibilityTest extends AtomicLongBasicTest {

    private Object atomicLongProxy;
    private Method getGroupIdMethod;

    @Override
    protected CPGroupId getGroupId(IAtomicLong ref) {
        try {
            findMethod(ref);
            Object groupId = getGroupIdMethod.invoke(atomicLongProxy);
            return (CPGroupId) HazelcastProxyFactory.proxyObjectForStarter(this.getClass().getClassLoader(), groupId);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private void findMethod(IAtomicLong ref)
            throws NoSuchMethodException {
        Class<?> atomicLongProxyClass;
        if (Proxy.isProxyClass(ref.getClass())) {
            // the IAtomicLong is loaded from a compatibility classloader
            ProxyInvocationHandler handler = (ProxyInvocationHandler) Proxy.getInvocationHandler(ref);
            // the delegate is the real AtomicLongProxy
            Object delegate = handler.getDelegate();
            atomicLongProxy = delegate;
            atomicLongProxyClass = delegate.getClass();
        } else {
            atomicLongProxyClass = AtomicLongProxy.class;
            atomicLongProxy = ref;
        }
        getGroupIdMethod = atomicLongProxyClass.getDeclaredMethod("getGroupId");
    }
}
