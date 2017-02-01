package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static info.jerrinot.compatibilityguardian.Utils.debug;


public class HazelcastProxyFactory {
    public static HazelcastInstance proxy(Object hazelcastDelegate) {
        Class<HazelcastInstance> expectedInterface = HazelcastInstance.class;
        HazelcastInstance proxy = generateProxyForInterface(hazelcastDelegate, expectedInterface);
        return proxy;
    }

    private static <T> T generateProxyForInterface(Object delegate, Class<?>...expectedInterfaces) {
        if (!checkImplementInterfaces(delegate, expectedInterfaces)) {
            throw new GuardianException("Cannot create proxy for class " + delegate);
        }
        InvocationHandler myInvocationHandler = new MyInvocationHandler(delegate);
        ClassLoader classloader = HazelcastProxyFactory.class.getClassLoader();
        return (T) Proxy.newProxyInstance(classloader, expectedInterfaces, myInvocationHandler);
    }

    private static boolean checkImplementInterfaces(Object o, Class<?>...ifaces) {
//        for (Class<?> iface : ifaces) {
//            if (!checkImplementInterface(o, iface)) {
//                return false;
//            }
//        }
        return true;
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


        private MyInvocationHandler(Object delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            debug("Proxy " + this + " called. Method: " + method);
            Class<?> delegateClass = delegate.getClass();
            Method methodDelegate = delegateClass.getMethod(method.getName(), method.getParameterTypes());

            Object delegateResult = methodDelegate.invoke(delegate, args);
            Class<?> returnType = method.getReturnType();
            // if there return types are equals -> they are loaded
            // by the same classloader -> no need to proxy what it returns
            if (methodDelegate.getReturnType().equals(returnType)) {
                return delegateResult;
            }

            //at this point we know the delegate return something loaded by
            //different class then the proxy -> we need to proxy the result
            Class<?>[] interfaces = getInterfaceForResultProxy(returnType);
            Object resultingProxy = HazelcastProxyFactory.generateProxyForInterface(delegateResult, interfaces);
            printInfoAboutResultProxy(resultingProxy);
            return resultingProxy;
        }

        private Class<?>[] getInterfaceForResultProxy(Class<?> returnType) {
            Class<?>[] interfaces = returnType.getInterfaces();
            //if the return type itself is an interface then we have to add it
            //to the list of interfaces implemented by the proxy
            if (returnType.isInterface()) {
                interfaces = Utils.concatItems(interfaces, returnType);
            }
            return interfaces;
        }

        private static void printInfoAboutResultProxy(Object resultingProxy) {
            debug("Returning proxy " + resultingProxy + ", loaded by " + resultingProxy.getClass().getClassLoader());
            Class<?>[] ifaces = resultingProxy.getClass().getInterfaces();
            debug("The proxy implementes intefaces: ");
            for (Class<?> iface : ifaces) {
                debug(iface + ", loaded by " + iface.getClassLoader());
            }
        }
    }
}
