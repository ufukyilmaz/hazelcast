package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static info.jerrinot.compatibilityguardian.Utils.rethrow;

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
            Class<?> returnType = method.getReturnType();

            Class<?> aClass = delegate.getClass();
            Method methodDelegate = aClass.getMethod(method.getName(), method.getParameterTypes());
            Object nestedDelegate = methodDelegate.invoke(delegate, args);

            Class<?>[] interfaces = returnType.getInterfaces();


            Object resultingProxy = HazelcastProxyFactory.generateProxyForInterface(nestedDelegate, interfaces);
            System.out.println("Returning proxy " + resultingProxy + ", loaded by " + resultingProxy.getClass().getClassLoader());
            Class<?>[] ifaces = resultingProxy.getClass().getInterfaces();
            System.out.println("The proxy implementes intefaces: ");
            for (Class<?> iface : ifaces) {
                System.out.println(iface + ", loaded by " + iface.getClassLoader());
            }
            return resultingProxy;
//            return null;
        }
    }
}
