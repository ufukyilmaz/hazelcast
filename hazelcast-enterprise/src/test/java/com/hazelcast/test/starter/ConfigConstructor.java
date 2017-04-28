package com.hazelcast.test.starter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Clone the configuration from {@code mainConfig} to a new configuration object loaded in the
 * target {@code classloader}. The returned configuration has its classloader set to the target classloader.
 */
public class ConfigConstructor extends AbstractStarterObjectConstructor {

    public ConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Object otherConfig = cloneConfig(delegate, classloader);

        Method setClassLoaderMethod = targetClass.getMethod("setClassLoader", ClassLoader.class);
        setClassLoaderMethod.invoke(otherConfig, classloader);
        return otherConfig;
    }

    private static boolean isGetter(Method method) {
        if (!method.getName().startsWith("get") && !method.getName().startsWith("is")) {
            return false;
        }
        if (method.getParameterTypes().length != 0) {
            return false;
        }
        if (void.class.equals(method.getReturnType())) {
            return false;
        }
        return true;
    }

    private static Object cloneConfig(Object thisConfigObject, ClassLoader classloader)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, InvocationTargetException {
        if (thisConfigObject == null) {
            return null;
        }

        Class thisConfigClass = thisConfigObject.getClass();
        if (isImmutableJavaType(thisConfigClass)) {
            return thisConfigObject;
        }

        Class<?> otherConfigClass = classloader.loadClass(thisConfigClass.getName());
        Object otherConfigObject = otherConfigClass.newInstance();

        for (Method method : thisConfigClass.getMethods()) {
            Class returnType = method.getReturnType();
            Method setter;
            if (isGetter(method)
                    && (setter = getSetter(otherConfigClass, getOtherReturnType(classloader, returnType), createSetterName(method))) != null) {

                if (Properties.class.isAssignableFrom(returnType)) {
                    //ignore
                } else if (Map.class.isAssignableFrom(returnType) || ConcurrentMap.class.isAssignableFrom(returnType)) {
                    Map map = (Map) method.invoke(thisConfigObject, null);
                    Map otherMap = ConcurrentMap.class.isAssignableFrom(returnType) ? new ConcurrentHashMap() : new HashMap();
                    for (Object entry : map.entrySet()) {
                        String key = (String) ((Map.Entry) entry).getKey();
                        Object value = ((Map.Entry) entry).getValue();
                        Object otherMapItem = cloneConfig(value, classloader);
                        otherMap.put(key, otherMapItem);
                    }
                    updateConfig(setter, otherConfigObject, otherMap);
                } else if (returnType.equals(List.class)) {
                    List list = (List) method.invoke(thisConfigObject, null);
                    List otherList = new ArrayList();
                    for (Object item : list) {
                        Object otherItem = cloneConfig(item, classloader);
                        otherList.add(otherItem);
                    }
                    updateConfig(setter, otherConfigObject, otherList);
                } else if (returnType.isEnum()) {
                    Enum thisSubConfigObject = (Enum) method.invoke(thisConfigObject, null);
                    Class otherEnumClass = classloader.loadClass(thisSubConfigObject.getClass().getName());
                    Object otherEnumValue = Enum.valueOf(otherEnumClass, thisSubConfigObject.name());
                    updateConfig(setter, otherConfigObject, otherEnumValue);
                } else if (returnType.getName().startsWith("java") || returnType.isPrimitive()) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    updateConfig(setter, otherConfigObject, thisSubConfigObject);
                } else if (returnType.getName().startsWith("com.hazelcast.memory.MemorySize")) {
                    //ignore
                } else if (returnType.getName().startsWith("com.hazelcast")) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    Object otherSubConfig = cloneConfig(thisSubConfigObject, classloader);
                    updateConfig(setter, otherConfigObject, otherSubConfig);
                } else {
                    //
                }
            }
        }
        return otherConfigObject;
    }

    private static boolean isImmutableJavaType(Class type) {
        if (type.isPrimitive()) {
            return true;
        }
        if (type == String.class) {
            return true;
        }
        if (type == Boolean.class) {
            return true;
        }
        if (Number.class.isAssignableFrom(type)) {
            return true;
        }
        return false;
    }

    private static Class<?> getOtherReturnType(ClassLoader classloader, Class returnType)
            throws ClassNotFoundException {
        String returnTypeName = returnType.getName();
        if (returnTypeName.startsWith("com.hazelcast")) {
            return classloader.loadClass(returnTypeName);
        }
        return returnType;
    }

    private static Method getSetter(Class otherConfigClass, Class returnType, String setterName) {
        try {
            return otherConfigClass.getMethod(setterName, returnType);
        } catch (NoSuchMethodException e) {
        }
        return null;
    }

    private static void updateConfig(Method setterMethod, Object otherConfigObject, Object value) {
        try {
            setterMethod.invoke(otherConfigObject, value);
        } catch (IllegalAccessException e) {
        } catch (InvocationTargetException e) {
        } catch (IllegalArgumentException e) {
            System.out.println(setterMethod);
            System.out.println(e);
        }
    }

    private static String createSetterName(Method getter) {
        if (getter.getName().startsWith("get")) {
            return "s" + getter.getName().substring(1);
        }
        if (getter.getName().startsWith("is")) {
            return "set" + getter.getName().substring(2);
        }
        throw new IllegalArgumentException("Unknown getter method name: " + getter.getName());
    }

    public static Object getValue(Object obj, String getter)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = obj.getClass().getMethod(getter, null);
        return method.invoke(obj, null);
    }
}
