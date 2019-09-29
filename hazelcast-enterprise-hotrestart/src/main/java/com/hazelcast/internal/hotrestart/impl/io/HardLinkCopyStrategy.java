package com.hazelcast.internal.hotrestart.impl.io;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * File copy strategy which uses hard links from JDK7
 * (see https://docs.oracle.com/javase/7/docs/api/java/nio/file/Files.html#createLink(java.nio.file.Path,%20java.nio.file.Path)).
 * The availability of hard links and usefulness of this strategy depends on whether you are running on JDK7 or higher and if
 * the requirements by the {@code createLink} method are satisfied.
 * If the source file changes after the copy operation has finished, the contents of the target file will change as well but if
 * the source file is deleted or renamed, the target file will remain unchanged and still existant. Thus this strategy is most
 * useful for files that never change and are copy-on-write.
 * The constructor of this strategy will throw an {@link UnsupportedOperationException} if the relevant classes or methods
 * were not found.
 */
public class HardLinkCopyStrategy implements FileCopyStrategy {
    private final Method toPathMethod;
    private final Method createLinkMethod;

    /**
     * Constructs the strategy or throws an exception if the relevant classes or methods are not available :
     * <ul>
     *     <li>java.nio.file.Files#createLink(java.nio.file.Path, java.nio.file.Path)</li>
     *     <li>java.io.File#toPath()</li>
     * </ul>
     *
     * @throws UnsupportedOperationException if the relevant classes or methods were not found
     */
    public HardLinkCopyStrategy() {
        try {
            final Class<?> files = Class.forName("java.nio.file.Files");
            final Class<?> path = Class.forName("java.nio.file.Path");
            toPathMethod = File.class.getMethod("toPath");
            createLinkMethod = files.getMethod("createLink", path, path);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException("createLink is not suppported. Is Hazelcast running on JDK 7 or higher?", e);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("createLink is not suppported because methods were not found", e);
        }
    }

    @Override
    public void copy(File source, File target) {
        try {
            final File destination = target.isDirectory() ? new File(target, source.getName()) : target;
            createLinkMethod.invoke(null, toPathMethod.invoke(destination), toPathMethod.invoke(source));
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException("createLink is not suppported", e);
        } catch (InvocationTargetException e) {
            throw new UnsupportedOperationException("createLink is not suppported", e);
        }
    }
}
