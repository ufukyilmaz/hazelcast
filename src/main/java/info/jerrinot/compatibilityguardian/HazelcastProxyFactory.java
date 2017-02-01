package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static info.jerrinot.compatibilityguardian.Utils.rethrow;

public class HazelcastProxyFactory {
    public static HazelcastInstance proxy(Object hazelcastDelegate) {
        if (!checkImplementInterface(hazelcastDelegate, HazelcastInstance.class)) {
            throw new GuardianException("Cannot create proxy for class " + hazelcastDelegate);
        }
        InvocationHandler myInvocationHandler = new MyInvocationHandler(hazelcastDelegate);
        ClassLoader classloader = HazelcastProxyFactory.class.getClassLoader();
        HazelcastInstance proxy = (HazelcastInstance) Proxy.newProxyInstance(classloader, new Class[]{HazelcastInstance.class}, myInvocationHandler);
        return proxy;
    }

    private static boolean checkImplementInterface(Object o, Class<?> iface) {
        Class<?>[] interfaces = o.getClass().getInterfaces();
        for (Class<?> current : interfaces) {
            if (current.getName().equals(iface.getName())) {
                return true;
            }
        }
        return false;
    }

    private static class MyInvocationHandler implements InvocationHandler {
        private final Object delegate;

        private final Method shutdownMethod;

        private MyInvocationHandler(Object delegate) {
            this.delegate = delegate;
            Class<?> delegateClass = delegate.getClass();
            try {
                this.shutdownMethod = delegateClass.getMethod("shutdown");
            } catch (NoSuchMethodException e) {
                throw rethrow(e);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Class<?> aClass = delegate.getClass();
            Method methodDelegate = aClass.getMethod(method.getName(), method.getParameterTypes());
            return methodDelegate.invoke(delegate, args);
        }
    }
}
