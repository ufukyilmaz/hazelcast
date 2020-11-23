package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionlessSemaphoreProxy;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.test.starter.HazelcastProxyFactory;
import com.hazelcast.test.starter.ProxyInvocationHandler;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Compatibility test for Semaphore.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class SemaphoreCompatibilityTest extends SessionlessSemaphoreBasicTest {

    private Object semaphoreProxy;
    private Method getGroupIdMethod;

    @Ignore
    @Override
    @Test
    public void testIncreasePermits_notifiesPendingAcquires() {
        // FIXME ignored as it requires access to RaftNodeImpl
        //  that is not currently implemented for proxied
        //  HazelcastInstances
        super.testIncreasePermits_notifiesPendingAcquires();
    }

    @Override
    protected CPGroupId getGroupId(ISemaphore ref) {
        try {
            findMethod(ref);
            Object groupId = getGroupIdMethod.invoke(semaphoreProxy);
            return (CPGroupId) HazelcastProxyFactory.proxyObjectForStarter(this.getClass().getClassLoader(), groupId);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private void findMethod(ISemaphore ref)
            throws NoSuchMethodException {
        Class<?> semaphoreProxyClass;
        if (Proxy.isProxyClass(ref.getClass())) {
            // the ISemaphore is loaded from a compatibility classloader
            ProxyInvocationHandler handler = (ProxyInvocationHandler) Proxy.getInvocationHandler(ref);
            // the delegate is the real SessionlessSemaphoreProxy
            Object delegate = handler.getDelegate();
            semaphoreProxy = delegate;
            semaphoreProxyClass = delegate.getClass();
        } else {
            semaphoreProxyClass = SessionlessSemaphoreProxy.class;
            semaphoreProxy = ref;
        }
        getGroupIdMethod = semaphoreProxyClass.getMethod("getGroupId");
    }
}
