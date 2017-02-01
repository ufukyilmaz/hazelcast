package info.jerrinot.compatibilityguardian;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ThreadLocalRandom;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;

import static info.jerrinot.compatibilityguardian.Utils.rethrow;

public class HazelcastStarter {
    public static HazelcastInstance startHazelcastVersion(String version) {
        File versionDir = getOrCreateVersionVersionDirectory(version);
        File[] files = Downloader.downloadVersion(version, versionDir);
        URL[] urls = fileIntoUrls(files);
        ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.core.Hazelcast");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
            Object config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastInstance", configClass);
            return (HazelcastInstance) newHazelcastInstanceMethod.invoke(null, config);
//            return null;

        } catch (ClassNotFoundException e) {
            throw rethrow(e);
        } catch (NoSuchMethodException e) {
            throw rethrow(e);
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        } catch (InvocationTargetException e) {
            throw rethrow(e);
        } catch (InstantiationException e) {
            throw rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private static URL[] fileIntoUrls(File[] files) {
        URL[] urls = new URL[files.length];
        for (int i = 0; i < files.length; i++) {
            try {
                urls[i] = files[i].toURL();
            } catch (MalformedURLException e) {
                throw rethrow(e);
            }
        }
        return urls;
    }

    private static File getOrCreateVersionVersionDirectory(String version) {
        File workingDir = new File(Configuration.WORKING_DIRECTORY);
        if (!workingDir.isDirectory() || !workingDir.exists()) {
            throw new GuardianException("Working directory " + workingDir + " does not exist.");
        }

        File versionDir = new File(Configuration.WORKING_DIRECTORY, version);
        versionDir.mkdir();
        return versionDir;
    }
}
