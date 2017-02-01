package info.jerrinot.compatibilityguardian;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static info.jerrinot.compatibilityguardian.Utils.concatItems;
import static info.jerrinot.compatibilityguardian.Utils.debug;
import static info.jerrinot.compatibilityguardian.Utils.rethrow;

class ProxyInvocationHandler implements InvocationHandler {
    private final Object delegate;


    ProxyInvocationHandler(Object delegate) {
        this.delegate = delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        debug("Proxy " + this + " called. Method: " + method);
        Class<?> delegateClass = delegate.getClass();
        Method methodDelegate = getMethodDelegate(method, delegateClass);

        Class<?> returnType = method.getReturnType();
        ClassLoader delegateClassClassLoader = delegateClass.getClassLoader();
        Object[] newArgs = proxyArgumentsIfNeeded(proxy, args, delegateClassClassLoader);
        Object delegateResult = invokeMethodDelegate(methodDelegate, newArgs);
        // if there return types are equals -> they are loaded
        // by the same classloader -> no need to proxy what it returns
        if (methodDelegate.getReturnType().equals(returnType) || delegateResult == null) {
            return delegateResult;
        }

        //at this point we know the delegate return something loaded by
        //different class then the proxy -> we need to proxy the result
        Class<?>[] interfaces = getAllInterfaces(returnType);
        ClassLoader targetClassLoader = proxy.getClass().getClassLoader();
        Object resultingProxy = HazelcastProxyFactory.generateProxyForInterface(delegateResult, targetClassLoader, interfaces);
        printInfoAboutResultProxy(resultingProxy);
        return resultingProxy;
    }

    private Object[] proxyArgumentsIfNeeded(Object proxy, Object[] args, ClassLoader delegateClassClassLoader) throws ClassNotFoundException {
        if (args == null) {
            return null;
        }

        Object[] newArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null || arg.getClass().getClassLoader() == String.class.getClassLoader()) {
                newArgs[i] = arg;
            } else {
                Class<?>[] ifaces = getAllInterfaces(arg.getClass());
                Class<?>[] delegateIfaces = new Class<?>[ifaces.length];
                for (int j = 0; j < ifaces.length; j++) {
                    Class<?> clazz = ifaces[j];
                    Class<?> delegateInterface = delegateClassClassLoader.loadClass(clazz.getName());
                    delegateIfaces[j] = delegateInterface;
                }
                Object newArg = HazelcastProxyFactory.generateProxyForInterface(arg, delegateClassClassLoader, delegateIfaces);
                newArgs[i] = newArg;
            }
        }
        return newArgs;
    }

    private Object invokeMethodDelegate(Method methodDelegate, Object[] args) throws Throwable {
        Object delegateResult;
        try {
            methodDelegate.setAccessible(true);
            delegateResult = methodDelegate.invoke(delegate, args);
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
        return delegateResult;
    }

    private Method getMethodDelegate(Method method, Class<?> delegateClass) {
        Method methodDelegate;
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes != null) {
            for (int i = 0; i < parameterTypes.length; i++) {
                Class<?> parameterType = parameterTypes[i];
                ClassLoader parameterTypeClassloader = parameterType.getClassLoader();
                ClassLoader delegateClassLoader = delegateClass.getClassLoader();
                if (parameterTypeClassloader != String.class.getClassLoader() && parameterTypeClassloader != delegateClassLoader) {
                    try {
                        Class<?> delegateParameterType = delegateClassLoader.loadClass(parameterType.getName());
                        parameterTypes[i] = delegateParameterType;
                    } catch (ClassNotFoundException e) {
                        throw rethrow(e);
                    }
                }
            }
        }
        try {
            methodDelegate = delegateClass.getMethod(method.getName(), parameterTypes);
        } catch (NoSuchMethodException e) {
            throw rethrow(e);
        }
        return methodDelegate;
    }

    private Class<?>[] getAllInterfaces(Class<?> type) {
        Class<?>[] interfaces = type.getInterfaces();
        //if the return type itself is an interface then we have to add it
        //to the list of interfaces implemented by the proxy
        if (type.isInterface()) {
            interfaces = concatItems(interfaces, type);
        }
        return interfaces;
    }

    private static void printInfoAboutResultProxy(Object resultingProxy) {
        if (!Utils.DEBUG_ENABLED) {
            return;
        }
        debug("Returning proxy " + resultingProxy + ", loaded by " + resultingProxy.getClass().getClassLoader());
        Class<?>[] ifaces = resultingProxy.getClass().getInterfaces();
        debug("The proxy implementes intefaces: ");
        for (Class<?> iface : ifaces) {
            debug(iface + ", loaded by " + iface.getClassLoader());
        }
    }
}
