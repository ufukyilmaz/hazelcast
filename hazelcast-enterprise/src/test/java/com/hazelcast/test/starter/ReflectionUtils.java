package com.hazelcast.test.starter;

import org.junit.Assert;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Reflection utilities
 */
public class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static Object getFieldValueReflectively(Object arg, String fieldName)
            throws IllegalAccessException {
        checkNotNull(arg, "Argument cannot be null");
        checkHasText(fieldName, "Field name cannot be null");

        Field field = getAllFieldsByName(arg.getClass()).get(fieldName);
        if (field == null) {
            throw new NoSuchFieldError("Field " + fieldName + " does not exist on object " + arg);
        }

        field.setAccessible(true);
        return field.get(arg);
    }

    public static void setFieldValueReflectively(Object arg, String fieldName, Object newValue)
            throws IllegalAccessException {
        checkNotNull(arg, "Argument cannot be null");
        checkHasText(fieldName, "Field name cannot be null");

        Field field = getAllFieldsByName(arg.getClass()).get(fieldName);
        if (field == null) {
            throw new NoSuchFieldError("Field " + fieldName + " does not exist on object " + arg);
        }

        field.setAccessible(true);
        field.set(arg, newValue);
    }

    public static Map<String, Field> getAllFieldsByName(Class<?> clazz) {
        ConcurrentMap<String, Field> fields = new ConcurrentHashMap<String, Field>();
        Field[] ownFields = clazz.getDeclaredFields();
        for (Field field : ownFields) {
            fields.put(field.getName(), field);
        }
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null) {
            ownFields = superClass.getDeclaredFields();
            for (Field field : ownFields) {
                fields.putIfAbsent(field.getName(), field);
            }
            superClass = superClass.getSuperclass();
        }
        return fields;
    }

}
