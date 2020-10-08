package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.datastructures.atomicref.proxy.AtomicRefProxy;
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
 * Compatibility test for AtomicReference.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicReferenceCompatibilityTest extends AtomicRefBasicTest {

    private Object atomicRefProxy;
    private Method getGroupIdMethod;

    @Override
    protected CPGroupId getGroupId(IAtomicReference ref) {
        try {
            findMethod(ref);
            Object groupId = getGroupIdMethod.invoke(atomicRefProxy);
            return (CPGroupId) HazelcastProxyFactory.proxyObjectForStarter(this.getClass().getClassLoader(), groupId);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private void findMethod(IAtomicReference ref)
            throws NoSuchMethodException {
        Class<?> atomicRefProxyClass;
        if (Proxy.isProxyClass(ref.getClass())) {
            // the IAtomicReference is loaded from a compatibility classloader
            ProxyInvocationHandler handler = (ProxyInvocationHandler) Proxy.getInvocationHandler(ref);
            // the delegate is the real AtomicRefProxy
            Object delegate = handler.getDelegate();
            atomicRefProxy = delegate;
            atomicRefProxyClass = delegate.getClass();
        } else {
            atomicRefProxyClass = AtomicRefProxy.class;
            atomicRefProxy = ref;
        }
        getGroupIdMethod = atomicRefProxyClass.getDeclaredMethod("getGroupId");
    }

}
