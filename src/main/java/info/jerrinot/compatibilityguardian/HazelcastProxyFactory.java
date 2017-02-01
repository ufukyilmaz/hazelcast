package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.HazelcastInstance;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;


public class HazelcastProxyFactory {
    public static HazelcastInstance proxy(Object hazelcastDelegate) {
        Class<HazelcastInstance> expectedInterface = HazelcastInstance.class;
        HazelcastInstance proxy = generateProxyForInterface(hazelcastDelegate, expectedInterface);
        return proxy;
    }

    public static <T> T generateProxyForInterface(Object delegate, Class<?>...expectedInterfaces) {
        if (!checkImplementInterfaces(delegate, expectedInterfaces)) {
            throw new GuardianException("Cannot create proxy for class " + delegate);
        }
        InvocationHandler myInvocationHandler = new ProxyInvocationHandler(delegate);
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

}
