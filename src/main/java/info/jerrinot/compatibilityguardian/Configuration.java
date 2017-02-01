package info.jerrinot.compatibilityguardian;

import com.hazelcast.config.Config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Configuration {
    public static final String WORKING_DIRECTORY = System.getProperty("user.home") + "/compatguardian";


    public static Object configLoader(Config mainConfig, ClassLoader classloader) {
        try {
            Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
            Object otherConfig = cloneConfig(mainConfig, classloader);

            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(otherConfig, classloader);
            return otherConfig;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
    private static boolean isGetter(Method method){
        if(!method.getName().startsWith("get"))      return false;
        if(method.getParameterTypes().length != 0)   return false;
        if(void.class.equals(method.getReturnType())) return false;
        return true;
    }

    public static Object cloneConfig(Object thisConfigObject, ClassLoader classloader) throws Exception{
        if(thisConfigObject == null) return null;

        Class thisConfigClass = thisConfigObject.getClass();

        Class<?> otherConfigClass = classloader.loadClass(thisConfigClass.getName());
        Object otherConfigObject = otherConfigClass.newInstance();

        for (Method method : thisConfigClass.getMethods()) {
            Class returnType = method.getReturnType();
            if(isGetter(method) && hasSetter(otherConfigClass, returnType, creatSetterName(method))){
                if (Properties.class.isAssignableFrom(returnType) ) {
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
                    updateConfig(otherConfigClass, returnType, creatSetterName(method), otherConfigObject, otherMap);
                } else if(returnType.equals(List.class)) {
                    List list = (List) method.invoke(thisConfigObject, null);
                    List otherList = new ArrayList();
                    for (Object item : list) {
                        Object otherItem = cloneConfig(item, classloader);
                        otherList.add(otherItem);
                    }
                    updateConfig(otherConfigClass, returnType, creatSetterName(method), otherConfigObject, otherList);
                } else if(returnType.isEnum()) {
                    Enum thisSubConfigObject = (Enum) method.invoke(thisConfigObject, null);
                    Class otherEnumClass = classloader.loadClass(thisSubConfigObject.getClass().getName());
                    Object otherEnumValue = Enum.valueOf(otherEnumClass, thisSubConfigObject.name());
                    updateConfig(otherConfigClass, returnType, creatSetterName(method), otherConfigObject, otherEnumValue);
                } else if(returnType.getName().startsWith("java")) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    updateConfig(otherConfigClass, returnType, creatSetterName(method), otherConfigObject, thisSubConfigObject);
                } else if(returnType.getName().startsWith("com.hazelcast.memory.MemorySize")) {
                    //ignore
                } else if(returnType.getName().startsWith("com.hazelcast")) {
                    Object thisSubConfigObject = method.invoke(thisConfigObject, null);
                    Object otherSubConfig= cloneConfig(thisSubConfigObject, classloader);
                    updateConfig(otherConfigClass, returnType, creatSetterName(method), otherConfigObject, otherSubConfig);
                } else {
                    //
                }
            }
        }
        return otherConfigObject;
    }

    private static boolean hasSetter(Class otherConfigClass, Class returnType, String setterName) {
        try {
            otherConfigClass.getMethod(setterName, returnType);
            return true;
        } catch (NoSuchMethodException e) {
        }
        return false;
    }

    private static void updateConfig(Class otherConfigClass, Class returnType, String setterName, Object otherConfigObject,
            Object value) {
        Method setterMethod = null;
        try {
            setterMethod = otherConfigClass.getMethod(setterName, returnType);
            setterMethod.invoke(otherConfigObject, value);
        } catch (NoSuchMethodException e) {
        } catch (IllegalAccessException e) {
        } catch (InvocationTargetException e) {
        } catch (IllegalArgumentException e) {
            System.out.println(setterMethod);
            System.out.println(e);
        }
    }

    private static String creatSetterName(Method getter) {
        return "s"+getter.getName().substring(1);
    }

    public static Object getValue(Object obj, String getter)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = obj.getClass().getMethod(getter, null);
        return method.invoke(obj, null);
    }
}
