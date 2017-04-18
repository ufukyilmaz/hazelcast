/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.test.starter;

import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Abstract superclass for {@code ConstructorFunction}s which, given a target {@code Class}, create an {@code Object} of
 * target {@code Class} off an input {@code Object}. For example, assuming a {@code Config} instance in current classloader,
 * the appropriate {@code ConstructorFunction} would create a {@code Config} object representing the same configuration for
 * the classloader that loads Hazelcast version 3.8.
 */
public abstract class AbstractStarterObjectConstructor implements ConstructorFunction<Object, Object> {

    protected final Class<?> targetClass;

    public AbstractStarterObjectConstructor(Class<?> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public Object createNew(Object arg) {
        try {
            return createNew0(arg);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    abstract Object createNew0(Object delegate) throws Exception;

    static Object getFieldValueReflectively(Object arg, String fieldName)
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

    private static Map<String, Field> getAllFieldsByName(Class<?> clazz) {
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
