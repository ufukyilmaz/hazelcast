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
        Object delegateResult = invokeMethodDelegate(methodDelegate, args);
        Class<?> returnType = method.getReturnType();
        // if there return types are equals -> they are loaded
        // by the same classloader -> no need to proxy what it returns
        if (methodDelegate.getReturnType().equals(returnType) || delegateResult == null) {
            return delegateResult;
        }

        //at this point we know the delegate return something loaded by
        //different class then the proxy -> we need to proxy the result
        Class<?>[] interfaces = getInterfaceForResultProxy(returnType);
        Object resultingProxy = HazelcastProxyFactory.generateProxyForInterface(delegateResult, interfaces);
        printInfoAboutResultProxy(resultingProxy);
        return resultingProxy;
    }

    private Object invokeMethodDelegate(Method methodDelegate, Object[] args) throws Throwable {
        Object delegateResult;
        try {
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
        try {
            methodDelegate = delegateClass.getMethod(method.getName(), method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            throw rethrow(e);
        }
        return methodDelegate;
    }

    private Class<?>[] getInterfaceForResultProxy(Class<?> returnType) {
        Class<?>[] interfaces = returnType.getInterfaces();
        //if the return type itself is an interface then we have to add it
        //to the list of interfaces implemented by the proxy
        if (returnType.isInterface()) {
            interfaces = concatItems(interfaces, returnType);
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
