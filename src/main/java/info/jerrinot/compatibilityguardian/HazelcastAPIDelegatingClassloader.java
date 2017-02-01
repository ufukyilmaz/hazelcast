package info.jerrinot.compatibilityguardian;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

import static info.jerrinot.compatibilityguardian.Utils.debug;

public class HazelcastAPIDelegatingClassloader extends URLClassLoader {
    private Object mutex = new Object();

    public HazelcastAPIDelegatingClassloader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        debug("Calling getResource with " + name);
        if (name.contains("hazelcast")) {
            return findResources(name);
        }
        return super.getResources(name);
    }

    @Override
    public URL getResource(String name) {
        debug("Getting resource " + name);
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
                    loadedClass = findClass(name);
                }
                //at this point it's always non-null.
                if (resolve) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            }
        }
    }

    private boolean shouldDelegate(String name) {
        if (!name.startsWith("com.hazelcast")) {
            return true;
        }
//        if (name.equals("com.hazelcast.core.Hazelcast")) {
//            return true;
//        }
//        if (name.equals("com.hazelcast.core.HazelcastInstance")) {
//            return true;
//        }
        return false;
    }
}
