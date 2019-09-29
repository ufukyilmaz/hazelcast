package com.hazelcast.internal.hotrestart.impl.di;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotates a method that the DI container will invoke after all the objects in the container had
 * their dependencies injected. This method can declare parameters in the same manner and with the
 * same semantics as {@link Inject}-annotated methods. <strong>The DI container makes no attempt to
 * ensure that the objects passed to this method are already initialized.</strong> The order of
 * initialization always happens in the order of object registration.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface Initialize {
}
