package com.hazelcast.internal.serialization.impl;

import com.hazelcast.cache.impl.wan.WanCacheEntryView;
import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class EnterpriseDataSerializableConventionsTest extends DataSerializableConventionsTest {

    private static final String[] WHITELISTED_PACKAGE_NAMES = new String[] {
            "com.hazelcast.com.fasterxml",
            "com.hazelcast.com.google.common",
            "com.hazelcast.org.apache.calcite",
            "com.hazelcast.org.codehaus",
            "com.hazelcast.org.slf4j"
    };

    // snakeyaml classes are compiled for java target 1.8
    private static final String[] WHITELISTED_JDK8_CLASS_NAMES = new String[] {
            "com.hazelcast.org.snakeyaml.engine.v1.exceptions.Mark",
            "com.hazelcast.org.snakeyaml.engine.v1.representer.BaseRepresenter$1",
    };

    @Override
    protected Set<String> getWhitelistedPackageNames() {
        return new HashSet<>(Arrays.asList(WHITELISTED_PACKAGE_NAMES));
    }

    @Override
    protected Set<Class> getWhitelistedClasses() {
        Set<Class> classes = super.getWhitelistedClasses();
        classes.add(WanCacheEntryView.class);

        if (JavaVersion.isAtLeast(JavaVersion.JAVA_8)) {
            // only resolve those classes when tests are executed with Java 8 or later
            for (String className : WHITELISTED_JDK8_CLASS_NAMES) {
                addToWhiteList(classes, className);
            }
        }
        return classes;
    }

    // whitelist class accessible only by class name
    private void addToWhiteList(Set<Class> whitelist, String className) {
        try {
            whitelist.add(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
