package com.hazelcast.internal.hotrestart.impl.di;

import com.hazelcast.internal.nio.Disposable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * <p>
 * A container which simplifies the process of "wiring" a set of collaborator objects (providing each object
 * with references to all its collaborators), as well as basic lifecycle management (initialization and
 * disposal).
 * </p><p>
 * Objects are registered with the container by calling one of the {@code dep(...)} methods. An object can be
 * registered either by type or by name. If no explicit name or type is provided, then the object's actual
 * type is used. Likewise, an object specifies its dependency by declaring its registered type or name.
 * Type hierarchy is not considered when resolving by type; a dependency must be referred to by its registered type.
 * </p><p>
 * The container can accept either an already constructed object or a class that it will instantiate
 * itself. It will instantiate a class by calling a constructor annotated with {@link Inject}. A class
 * <emph>must</emph> declare at least one such constructor and <emph>should</emph> declare exactly one
 * because the container will choose one arbitrarily.
 * </p><p>
 * There are two main types of wiring:
 * <ol><li>
 *     <strong>Early</strong>: dependencies are passed to the constructor. This allows the wiring of
 *     {@code final} fields, but is vulnerable to dependency cycles.
 * </li><li>
 *     <strong>Late</strong>: dependencies are injected into a pre-existing instance, by direct assignment to
 *     {@code @Inject}-annotated fields and/or by passing them to {@code @Inject}-annotated methods. This forces
 *     the fields to be mutable, but is unaffected by dependency cycles.
 * </li></ol>
 * A class is free to mix and match both types of wiring; for example, by preferring early wiring and late-injecting
 * only the dependencies involved in a cycle, thus maximizing the benefits of {@code final} fields.
 * </p><p>
 * An object may need some initialization that uses late-injected dependencies. This is the purpose of the
 * {@link Initialize} method annotation. {@link #wireAndInitializeAll()} will first late-wire all registered objects,
 * then call all {@code @Initialize} methods on each object. An {@code @Initialize} method can specify parameters,
 * these will be provided by the container with the same semantics as for {@code @Inject} methods.
 * Objects will be wired and initialized in the same order in which they were registered.
 * </p><p>
 * An object may also be submitted for automatic disposal by calling {@link #disposable()} immediately after
 * the {@code dep(...)} call that registers it. The object must implement {@link Disposable}. When this container's
 * {@code dispose()} method is called, it will in turn call {@code dispose()} on all the objects submitted for disposal,
 * in the order opposite to the order of submission. There is also a {@link #disposable(Disposable)} method which can
 * be called with any object, whether registered with the container or not.
 * </p><p>
 * This is the expected protocol for using the container:
 * <ol><li>
 *     Create a new container. It is in the <emph>uninitialized</emph> state.
 * </li><li>
 *     Call various {@code dep(...)} methods to populate the container. The objects being registered
 *     are early-wired only.
 * </li><li>
 *     Call {@link #wireAndInitializeAll()}. This triggers late-wiring and initialization of all the
 *     registered objects and promotes the container to the <emph>initialized</emph> state.
 * </li><li>
 *     Further {@code dep(...)} calls can be made; now the objects are immediately late-wired and initialized. Also
 *     {@link #initialize(Object)} can be called to initialize an object without registering it with the container.
 * </li></ol>
 * A <emph>parent</emph> container can be passed to the {@code DiContainer} constructor. It will be used
 * as a fallback container for any failed dependency lookup. The parent container is accessed in a strictly
 * read-only fashion.
 * </p>
 */
public final class DiContainer implements Disposable {
    private final DiContainer parent;
    private final Map<Class<?>, Object> depsByType = new HashMap<Class<?>, Object>();
    private final Map<String, Object> depsByName = new HashMap<String, Object>();
    private final List<Disposable> disposables = new ArrayList<Disposable>();

    private List<Object> initOrder = new ArrayList<Object>();
    private Object lastDep;

    /**
     * Creates an empty container.
     */
    public DiContainer() {
        this(null);
    }

    /**
     * Creates an empty container which has a parent container.
     */
    public DiContainer(DiContainer parent) {
        this.parent = parent;
    }

    /**
     * Registers an object by its actual type.
     * @param dep the object to register
     * @return {@code this}
     */
    public DiContainer dep(Object dep) {
        if (dep instanceof Class) {
            return dep((Class<?>) dep);
        }
        final Class<?> type = dep.getClass();
        depsByType.put(type, dep);
        onRegistration(dep);
        return this;
    }

    /**
     * Registers an object by type.
     * @param declaredType the type under which to register the object
     * @param dep the object to register
     * @return {@code this}
     */
    public <T> DiContainer dep(Class<? super T> declaredType, T dep) {
        depsByType.put(declaredType, declaredType.cast(dep));
        onRegistration(dep);
        return this;
    }

    /**
     * Registers an object by name.
     * @param name the name under which to register the object
     * @param dep the object to register
     * @return {@code this}
     */
    public <T> DiContainer dep(String name, T dep) {
        depsByName.put(name, dep);
        onRegistration(dep);
        return this;
    }

    /**
     * Instantiates a type and registers the instance under that type.
     * @param type the type to instantiate
     * @return {@code this}
     */
    public DiContainer dep(Class<?> type) {
        final Object dep = instantiate(type);
        depsByType.put(type, dep);
        onRegistration(dep);
        return this;
    }

    /**
     * Instantiates a type and registers the instance under the specified type.
     * @param declaredType the type under which to register the instance
     * @param actualType the type to instantiate
     * @return {@code this}
     */
    public <T> DiContainer dep(Class<? super T> declaredType, Class<T> actualType) {
        final Object dep = declaredType.cast(instantiate(actualType));
        depsByType.put(declaredType, dep);
        onRegistration(dep);
        return this;
    }

    /**
     * Instantiates a type and registers the instance under the specified name.
     * @param name the name under which to register the instance
     * @param type the type to instantiate
     * @return {@code this}
     */
    public DiContainer dep(String name, Class<?> type) {
        final Object dep = instantiate(type);
        depsByName.put(name, dep);
        onRegistration(dep);
        return this;
    }

    /**
     * Adds the object registered by the preceding {@code dep(...)} call to the list of objects to dispose
     * when this container is disposed.
     * @return {@code this}
     * @throws ClassCastException if the registered object does not implement {@link Disposable}.
     */
    public DiContainer disposable() {
        disposable((Disposable) lastDep);
        return this;
    }

    /**
     * Adds an object to the list of objects to dispose when this container is disposed.
     * @return {@code this}
     */
    public DiContainer disposable(Disposable target) {
        disposables.add(target);
        return this;
    }

    /**
     * Instantiates the given type by calling its {@code @Inject}-annotated constructor and
     * satisfying the constructor's dependencies. Does not register the instance with the container
     * and does not late-wire the instance.
     * @param type the type to instantiate.
     * @return the created instance
     */
    public <T> T instantiate(Class<T> type) {
        for (Constructor c : type.getDeclaredConstructors()) {
            if (c.getAnnotation(Inject.class) != null) {
                return newInstance((Constructor<T>) c, resolveDepArgs(c.getParameterTypes(), c.getParameterAnnotations()));
            }
        }
        throw new DiException("No constructor annotated with @Inject in " + type.getName());
    }

    /**
     * Late-wires the provided target object without registering it with the container.
     * @param target the target into which to inject its dependencies.
     * @return {@code target}
     */
    public <T> T wire(T target) {
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            injectFields(target, type);
            injectMethods(target, type);
        }
        return target;
    }

    /**
     * Calls all {@link Initialize}-annotated methods on the supplied object.
     */
    public void initialize(Object target) {
        for (Method m : target.getClass().getDeclaredMethods()) {
            if (m.getAnnotation(Initialize.class) != null) {
                invoke(target, m, resolveDepArgs(m.getParameterTypes(), m.getParameterAnnotations()));
            }
        }
    }

    /**
     * Late-wires all the objects registered with the container, then calls all {@link Initialize}-annotated methods
     * on each object. Objects are wired and initialized in the order in which they were registered.
     * <p>
     * It is an error to call this method on an already initialized container.
     * @return {@code this}
     */
    public DiContainer wireAndInitializeAll() {
        if (initOrder == null) {
            throw new DiException("Container already initialized");
        }
        for (Object target : initOrder) {
            wire(target);
        }
        for (Object target : initOrder) {
            initialize(target);
        }
        initOrder = null;
        return this;
    }

    public Object invoke(Object target, String methodName) {
        final Method m = findMethod(target, methodName);
        m.setAccessible(true);
        return invoke(target, m, resolveDepArgs(m.getParameterTypes(), m.getParameterAnnotations()));
    }

    /**
     * Looks up a registered object by type. If local lookup fails, delegates to the parent container (if any).
     * @param t the type to look up
     * @return the found object or {@code null} if none found
     */
    public <T> T get(Class<T> t) {
        final Object localDep = depsByType.get(t);
        return localDep != null ? t.cast(localDep)
                : parent != null ? parent.get(t)
                : null;
    }

    /**
     * Looks up a registered object by name. If local lookup fails, delegates to the parent container (if any).
     * @param name the name to look up
     * @return the found object or {@code null} if none found
     */
    public Object get(String name) {
        final Object localDep = depsByName.get(name);
        return localDep != null ? localDep
                : parent != null ? parent.get(name)
                : null;
    }

    /**
     * Looks up a registered object by name and casts it into the supplied type. If local lookup fails,
     * delegates to the parent container (if any). If local lookup succeeds but the cast fails, the
     * methods fails without delegating to the parent.
     * @param name the name to look up
     * @return the found object or {@code null} if none found
     * @throws ClassCastException if an object is found, but doesn't comply with the supplied type
     */
    public <T> T get(String name, Class<T> type) {
        return type.cast(get(name));
    }

    /**
     * Calls {@code dispose()} on all objects submitted for disposal. Objects are disposed in the order
     * opposite to the order in which they were submitted.
     */
    @Override
    public void dispose() {
        for (ListIterator<Disposable> iterator = disposables.listIterator(disposables.size());
             iterator.hasPrevious();) {
            iterator.previous().dispose();
        }
    }

    private void onRegistration(Object target) {
        lastDep = target;
        if (initOrder != null) {
            initOrder.add(target);
        } else {
            initialize(wire(target));
        }
    }


    private <T> T resolveDependency(Name nameAnnotation, Class<T> targetType) {
        if (nameAnnotation != null) {
            final String name = nameAnnotation.value();
            final T dep = targetType.cast(get(name));
            if (dep == null) {
                throw new DiException(String.format("Couldn't resolve dep with name '%s'", name));
            }
            return dep;
        }
        final T byType = get(targetType);
        if (byType == null) {
            throw new DiException(String.format("Couldn't resolve dep with type %s", targetType.getName()));
        }
        return byType;
    }

    private Object[] resolveDepArgs(Class[] parameterTypes, Annotation[][] paramAnns) {
        final Object[] args = new Object[parameterTypes.length];
        int i = 0;
        for (Class<?> t : parameterTypes) {
            args[i] = resolveDependency(findAnnotation(paramAnns[i], Name.class), t);
            i++;
        }
        return args;
    }

    private <T> T findAnnotation(Annotation[] anns, Class<T> annType) {
        for (Annotation ann : anns) {
            if (annType.isAssignableFrom(ann.getClass())) {
                return annType.cast(ann);
            }
        }
        return null;
    }

    private void injectFields(Object target, Class<?> type) {
        for (Field f : type.getDeclaredFields()) {
            if (f.getAnnotation(Inject.class) != null) {
                setField(target, f, resolveDependency(f.getAnnotation(Name.class), f.getType()));
            }
        }
    }

    private void injectMethods(Object target, Class<?> type) {
        for (Method m : type.getDeclaredMethods()) {
            if (m.getAnnotation(Inject.class) == null) {
                continue;
            }
            invoke(target, m, resolveDepArgs(m.getParameterTypes(), m.getParameterAnnotations()));
        }
    }

    private static void setField(Object target, Field field, Object value) {
        field.setAccessible(true);
        try {
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new DiException("Dependency injection failed", e);
        }
    }

    private static Object invoke(Object target, Method method, Object[] args) {
        method.setAccessible(true);
        try {
            return method.invoke(target, args);
        } catch (IllegalAccessException e) {
            throw new DiException("Dependency injection failed", e);
        } catch (InvocationTargetException e) {
            throw new DiException("Dependency injection failed", e.getCause());
        }
    }

    private Method findMethod(Object target, String methodName) {
        for (Method m : target.getClass().getDeclaredMethods()) {
            if (m.getName().equals(methodName)) {
                return m;
            }
        }
        throw new DiException(
                String.format("Method lookup failed: %s#%s()", target.getClass().getName(), methodName));
    }

    private <T> T newInstance(Constructor<T> cxor, Object[] args) {
        cxor.setAccessible(true);
        try {
            return cxor.newInstance(args);
        } catch (IllegalAccessException e) {
            throw new DiException("Instantiation failed", e);
        } catch (InstantiationException e) {
            throw new DiException("Instantiation failed", e);
        } catch (InvocationTargetException e) {
            throw new DiException("Instantiation failed", e.getCause());
        }
    }
}
