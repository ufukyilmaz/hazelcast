/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeContext;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public class HazelcastStarter {

    // Cache downloaded files & classloader used to load their classes per version string
    private static final ConcurrentMap<String, HazelcastVersionClassloaderFuture> loadedVersions =
            new ConcurrentHashMap<String, HazelcastVersionClassloaderFuture>();

    public static HazelcastInstance newHazelcastInstance(String version) {
        return newHazelcastInstance(version, null);
    }

    /**
     * Start a new {@link HazelcastInstance} of the given {@code version}, configured with the given {@code Config}.
     *
     * @param version           Hazelcast version to start; must be a published artifact on maven central
     * @param configTemplate    configuration object to clone on the target HazelcastInstance
     * @return
     */
    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate) {
        return newHazelcastInstance(version, configTemplate, null);
    }

    public static HazelcastInstance newHazelcastInstance(String version, Config configTemplate,
                                                         NodeContext nodeContextTemplate) {
        HazelcastAPIDelegatingClassloader versionClassLoader = null;
        HazelcastVersionClassloaderFuture future = loadedVersions.get(version);

        if (future != null) {
            versionClassLoader = future.get();
        }

        future = new HazelcastVersionClassloaderFuture(version);
        HazelcastVersionClassloaderFuture found = loadedVersions.putIfAbsent(version, future);

        if (found != null) {
            versionClassLoader = found.get();
        }

        if (versionClassLoader == null) {
            try {
                versionClassLoader = future.get();
            } catch (Throwable t) {
                loadedVersions.remove(version, future);
                throw rethrow(t);
            }
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            return newHazelcastMemberWithNetwork(configTemplate, versionClassLoader);
        } catch (ClassNotFoundException e) {
            throw Utils.rethrow(e);
        } catch (NoSuchMethodException e) {
            throw Utils.rethrow(e);
        } catch (IllegalAccessException e) {
            throw Utils.rethrow(e);
        } catch (InvocationTargetException e) {
            throw Utils.rethrow(e);
        } catch (InstantiationException e) {
            throw Utils.rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private static HazelcastInstance newHazelcastMemberWithNetwork(Config configTemplate,
                                                                   HazelcastAPIDelegatingClassloader classloader)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {
        Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.core.Hazelcast");
        System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
        Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
        Object config;
        config = getConfig(configTemplate, classloader, configClass);

        Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastInstance", configClass);
        Object delegate = newHazelcastInstanceMethod.invoke(null, config);

        return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);
    }

    private static Object getConfig(Config configTemplate, HazelcastAPIDelegatingClassloader classloader,
                                    Class<?> configClass)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException,
            InvocationTargetException, ClassNotFoundException {
        Object config;
        if (configTemplate == null) {
            config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);
        } else {
            config = Configuration.configForClassLoader(configTemplate, classloader);
        }
        return config;
    }

    public static HazelcastInstance newHazelcastClient(String version) {
        File versionDir = getOrCreateVersionVersionDirectory(version);
        File[] files = HazelcastVersionLocator.locateVersion(version, versionDir, true);
        URL[] urls = fileIntoUrls(files);
        ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.client.HazelcastClient");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.client.config.ClientConfig");
            Object config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastClient", configClass);
            Object delegate = newHazelcastInstanceMethod.invoke(null, config);
            return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);

        } catch (ClassNotFoundException e) {
            throw Utils.rethrow(e);
        } catch (NoSuchMethodException e) {
            throw Utils.rethrow(e);
        } catch (IllegalAccessException e) {
            throw Utils.rethrow(e);
        } catch (InvocationTargetException e) {
            throw Utils.rethrow(e);
        } catch (InstantiationException e) {
            throw Utils.rethrow(e);
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
                throw Utils.rethrow(e);
            }
        }
        return urls;
    }

    private static File getOrCreateVersionVersionDirectory(String version) {
        File workingDir = Configuration.WORKING_DIRECTORY;
        if (!workingDir.isDirectory() || !workingDir.exists()) {
            throw new GuardianException("Working directory " + workingDir + " does not exist.");
        }

        File versionDir = new File(Configuration.WORKING_DIRECTORY, version);
        versionDir.mkdir();
        return versionDir;
    }

    private static class HazelcastVersionClassloaderFuture {
        private final String version;

        private HazelcastAPIDelegatingClassloader classLoader;

        HazelcastVersionClassloaderFuture(String version) {
            this.version = version;
        }

        public HazelcastAPIDelegatingClassloader get() {
            if (classLoader != null) {
                return classLoader;
            }

            synchronized (this) {
                File versionDir = getOrCreateVersionVersionDirectory(version);
                File[] files = HazelcastVersionLocator.locateVersion(version, versionDir, true);
                URL[] urls = fileIntoUrls(files);
                ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
                classLoader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
                return classLoader;
            }
        }
    }
}
