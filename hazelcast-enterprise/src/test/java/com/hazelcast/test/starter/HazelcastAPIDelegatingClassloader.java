package com.hazelcast.test.starter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

import static com.hazelcast.nio.IOUtil.toByteArray;

public class HazelcastAPIDelegatingClassloader extends URLClassLoader {
    private Object mutex = new Object();

    public HazelcastAPIDelegatingClassloader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Utils.debug("Calling getResource with " + name);
        if (name.contains("hazelcast")) {
            return findResources(name);
        }
        return super.getResources(name);
    }

    @Override
    public URL getResource(String name) {
        Utils.debug("Getting resource " + name);
        if (name.contains("hazelcast")) {
            return findResource(name);
        }
        return super.getResource(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (shouldDelegate(name)) {
            return super.loadClass(name, resolve);
        } else {
            synchronized (mutex) {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null) {
                    // locate test class' bytes in the current codebase but load the class in this classloader
                    // so that the test class implements interfaces from the old Hazelcast version
                    // eg. EntryListener's, EntryProcessor's etc.
                    if (isHazelcastTestClass(name)) {
                        loadedClass = findClassInParentURLs(name);
                    }
                    if (loadedClass == null) {
                        loadedClass = findClass(name);
                    }
                }
                //at this point it's always non-null.
                if (resolve) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            }
        }
    }

    /**
     * Attempts to locate a class file as a resource in parent classpath, then loads the class in this classloader
     * @return
     */
    private Class<?> findClassInParentURLs(final String name) {
        String classFilePath = name.replaceAll("\\.", "/").concat(".class");
        InputStream classInputStream = getParent().getResourceAsStream(classFilePath);
        if (classInputStream != null) {
            byte[] classBytes = null;
            try {
                classBytes = toByteArray(classInputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (classBytes != null) {
                Class<?> klass = this.defineClass(name, classBytes, 0, classBytes.length);
                return klass;
            }
        }
        return null;
    }

    private boolean shouldDelegate(String name) {
        if (!name.startsWith("com.hazelcast")) {
            return true;
        }

        // the ProxyInvocationHandler is serialized/deserialized as part of user objects serialization
        // eg proxied EntryListeners, EntryProcessors etc
        if (name.equals("com.hazelcast.test.starter.ProxyInvocationHandler")) {
            return true;
        }

        return false;
    }

    private boolean isHazelcastTestClass(String name) {
        if (!name.startsWith("com.hazelcast")) {
            return false;
        }

        if ((name.contains("Test") || name.contains(".test")) && !name.contains("ProxyInvocationHandler")) {
            return true;
        }

        return false;
    }
}
