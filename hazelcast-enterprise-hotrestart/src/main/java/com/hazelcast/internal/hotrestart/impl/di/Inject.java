package com.hazelcast.internal.hotrestart.impl.di;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotates a field, method, or constructor that the DI container will assign/invoke. The constructor/method
 * can declare an arbitrary number of paramters. The type of the field/parameter must match the registered
 * type of an object in the DI container. The field/parameter can also be annotated with {@link Name}, in that
 * case the object to inject will be looked up by the specified name.
 */
@Retention(RUNTIME)
@Target({CONSTRUCTOR, METHOD, FIELD})
public @interface Inject {
}
